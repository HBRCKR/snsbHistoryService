package com.hbrc.snsb3history.relay

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.event.ContextClosedEvent
import org.springframework.context.event.EventListener
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPOutputStream

/**
 * history_outbox → S3 (docs/history-outbox-s3-plan.md 5절, docs/history-service.md).
 *
 * 종류마다 아웃박스에 [RelayProperties.batchSize] 건이 쌓였거나 가장 오래된 건이 [RelayProperties.flushIntervalMs] 를 기다렸으면,
 * 앞에서부터(occurred_at, id) batchSize 건을 `FOR UPDATE SKIP LOCKED` 로 잠그고 합계 [RelayProperties.maxBatchBytes] 까지
 * 한 줄 = 이력 한 건(JSON)으로 묶어 gzip 파일로 S3 에 올린 뒤 같은 트랜잭션에서 행을 지운다(크기를 넘는 뒤쪽 행은 다음 묶음으로).
 * 한 건이 그 크기보다 크면 그 한 건만 파일 하나로 올린다 — 크기는 실패가 아니다.
 *
 * 실패 처리 — dead 는 "그 행만 안 되는" 게 확실할 때만, 장애는 아웃박스에 그대로 두고 기다린다:
 * - 형식이 깨진 행(change_type 없음, payload 가 객체가 아님): 바로 history_outbox_dead 로. 다시 해도 같다
 * - 묶음 쓰기 실패: 한 건씩 나눠 다시 쓴다(한 건마다 [RelayProperties.rowTries] 번까지). 성공한 행은 올리고,
 *   실패한 행은 **그 뒤에 성공한 행이 있을 때만** dead 로 보낸다 — 뒤의 행이 됐다는 건 S3·DB 가 멀쩡하다는 뜻이라 그 행 탓이다
 * - 뒤에 성공이 없는 실패(묶음 끝·전부 실패)는 판단을 미룬다 — 아웃박스에 그대로 두고 backoff. 다음 묶음에서 앞에 서서 다시 가려진다
 * - [RelayProperties.outageThreshold] 건이 연달아 실패하면 더 시도하지 않는다(장애로 보고 헛시도를 줄인다)
 * - 권한·버킷 없음 같은 설정 오류는 나눠 보지 않고 바로 장애로 처리하고 지표를 따로 올린다
 *
 * - 유실 없음: 행은 업로드가 성공한 뒤에만 지운다
 * - 중복 가능(at-least-once): 업로드 뒤 커밋 전에 죽거나 롤백되면 다시 올라간다 — 읽을 때 event_id 로 거른다
 * - 종료: 닫힐 때 새 묶음을 잡지 않고, 진행 중인 묶음의 업로드 → 삭제 → 커밋까지 마친다(스케줄러가 기다린다)
 * - 여러 대: SKIP LOCKED 로 서로 다른 행을 가져간다
 */
@Component
@ConditionalOnProperty(name = ["backfill.enabled"], havingValue = "false", matchIfMissing = true)
class OutboxRelay(
    private val jdbc: JdbcTemplate,
    transactionManager: PlatformTransactionManager,
    private val uploader: HistoryUploader,
    private val relay: RelayProperties,
    private val metrics: OutboxMetrics,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val transaction = TransactionTemplate(transactionManager)
    private val savepoint = TransactionTemplate(transactionManager).apply {
        propagationBehavior = TransactionDefinition.PROPAGATION_NESTED
    }
    private val stopping = AtomicBoolean(false)
    private val tableInsertSql = ConcurrentHashMap<HistoryTarget, String>()
    private var backoffMs = relay.backoffInitialMs
    private var retryAtMs = 0L

    /** 묶음에 담는 한 줄 */
    private class Line(val id: Long, val occurredAt: OffsetDateTime, val text: String)

    /** 장애(S3·네트워크·DB) — 행 탓이 아니라 묶음 전체를 롤백하고 기다린다 */
    private class OutageException(cause: Exception) : RuntimeException(cause.message, cause)

    @EventListener(ContextClosedEvent::class)
    fun stop() = stopping.set(true)

    @Scheduled(fixedDelayString = "\${relay.poll-interval-ms:10000}")
    fun poll() {
        if (stopping.get() || System.currentTimeMillis() < retryAtMs) return
        try {
            HistoryTarget.entries.forEach { target ->
                // 올릴 때가 된 동안 이어서 비운다(가득 찼거나 오래 기다린 이력이 남아 있는 동안)
                while (!stopping.get() && isDue(target) && relayBatch(target) > 0) Unit
            }
            backoffMs = relay.backoffInitialMs
        } catch (e: Exception) {
            if (generateSequence<Throwable>(e) { it.cause }.any { it is SdkException }) metrics.s3Failed()
            retryAtMs = System.currentTimeMillis() + backoffMs
            log.warn("이력 업로드 실패(장애) — {}ms 뒤 다시 시도한다: {}", backoffMs, e.message)
            backoffMs = minOf(backoffMs * 2, relay.backoffMaxMs)
        }
    }

    /** 올릴 때가 됐나 — 앞에서 batchSize 건만 보고, 가득 찼거나 가장 오래된 건이 flushIntervalMs 이상 기다렸으면 참 */
    private fun isDue(target: HistoryTarget): Boolean =
        jdbc.queryForObject(DUE_SQL, Boolean::class.java, relay.batchSize, relay.flushIntervalMs, target.name, relay.batchSize) == true

    /** 한 묶음을 올리고 지운다. 잠근 행 수를 돌려준다(0 이면 더 없음). 판단을 미룬 실패가 있으면 커밋 뒤 장애로 알린다 */
    fun relayBatch(target: HistoryTarget): Int {
        var deferred: Exception? = null
        val locked = transaction.execute {
            lockAndDeliver(target) { deferred = it }
        } ?: 0
        // 성공한 행은 커밋된 뒤 — 남은 실패는 장애일 수 있으니 기다렸다 다시 본다
        deferred?.let { throw OutageException(it) }
        return locked
    }

    private fun lockAndDeliver(target: HistoryTarget, defer: (Exception) -> Unit): Int {
        val lines = mutableListOf<Line>()
        val malformed = mutableListOf<Long>()
        var bytes = 0L
        var locked = 0
        jdbc.query(
            { con ->
                con.prepareStatement(LOCK_SQL).apply {
                    fetchSize = FETCH_SIZE
                    setInt(1, SCHEMA_VERSION)
                    setString(2, target.typeName)
                    setString(3, target.name)
                    setInt(4, relay.batchSize)
                }
            },
            RowCallbackHandler { rs ->
                locked++
                val id = rs.getLong(ID)
                val size = rs.getLong(BYTES)
                when {
                    !rs.getBoolean(WELL_FORMED) -> malformed.add(id)
                    // 크기를 넘는 뒤쪽 행은 잠가만 두고 다음 묶음으로(첫 줄은 혼자라도 담는다)
                    lines.isNotEmpty() && bytes + size > relay.maxBatchBytes -> Unit
                    else -> {
                        bytes += size
                        lines.add(Line(id, rs.getObject(OCCURRED_AT, OffsetDateTime::class.java), rs.getString(LINE)))
                    }
                }
            },
        )
        malformed.forEach { moveToDead(target, it, MALFORMED) }
        if (lines.isNotEmpty()) deliverOrIsolate(target, lines, defer)
        return locked
    }

    /** 묶음을 한 번에 쓰고, 실패하면 한 건씩 나눠 그 행 탓이 확실한 것만 dead 로 */
    private fun deliverOrIsolate(target: HistoryTarget, lines: List<Line>, defer: (Exception) -> Unit) {
        try {
            savepoint.executeWithoutResult { deliver(target, lines) }
            return
        } catch (e: Exception) {
            if (isConfigError(e)) {
                metrics.s3ConfigError()
                log.error("{} 이력 업로드 — S3 설정 오류(권한·버킷)라 사람이 고쳐야 한다: {}", target, e.message)
                throw OutageException(e)
            }
            log.warn("{} 묶음 {}건 쓰기 실패 — 한 건씩 나눠 다시 쓴다: {}", target, lines.size, e.message)
        }
        val poison = mutableListOf<Pair<Line, Exception>>()
        val pending = mutableListOf<Pair<Line, Exception>>()
        for (line in lines) {
            // 연달아 안 되면 장애로 보고 더 시도하지 않는다 — 남은 행은 아웃박스에 그대로
            if (pending.size >= relay.outageThreshold) break
            val error = deliverOne(target, line)
            if (error == null) {
                // 뒤의 행이 됐다 = S3·DB 는 멀쩡하다 → 앞에서 실패한 행은 그 행 탓
                poison.addAll(pending)
                pending.clear()
            } else {
                pending.add(line to error)
            }
        }
        poison.forEach { (line, error) -> moveToDead(target, line.id, rootMessage(error)) }
        // 뒤에 성공이 없는 실패는 판단을 미룬다(아웃박스에 그대로) — 성공한 행만 커밋하고 장애로 기다린다
        pending.lastOrNull()?.let { (_, error) -> defer(error) }
    }

    /** 한 건 쓰기 — [RelayProperties.rowTries] 번까지. 성공하면 null, 끝내 실패하면 마지막 오류 */
    private fun deliverOne(target: HistoryTarget, line: Line): Exception? {
        var last: Exception? = null
        repeat(relay.rowTries) {
            try {
                savepoint.executeWithoutResult { deliver(target, listOf(line)) }
                return null
            } catch (e: Exception) {
                if (isConfigError(e)) throw OutageException(e)
                last = e
            }
        }
        return last
    }

    /**
     * 이중 쓰기 → S3 업로드(시간 칸마다 파일 하나) → 아웃박스에서 지우기.
     * DB 쪽(이중 쓰기)을 먼저 해서, 거기서 실패하는 묶음·행은 S3 에 올라가지 않게 한다(롤백하면 DB 쪽도 같이 취소된다).
     */
    private fun deliver(target: HistoryTarget, lines: List<Line>) {
        val ids = lines.map { it.id }
        if (relay.dualWriteTable && target.hasHistoryTable) jdbc.update(tableInsertSql(target), idsSetter(ids))
        val files = HourlyJsonLines()
        lines.forEach { files.add(it.occurredAt, it.id, it.text) }
        files.upload(uploader, target, ::idFileName)
        jdbc.update(DELETE_SQL, idsSetter(ids))
        metrics.relayed(target, ids.size)
    }

    /** 그 행만 계속 안 된다 — 다른 이력을 막지 않게 dead 로 치워 둔다(원인을 고친 뒤 outbox 로 되돌린다) */
    private fun moveToDead(target: HistoryTarget, id: Long, reason: String) {
        val one = listOf(id)
        jdbc.update(DEAD_SQL, PreparedStatementSetter { ps ->
            ps.setString(1, reason)
            ps.setArray(2, ps.connection.createArrayOf(BIGINT, one.toTypedArray()))
        })
        jdbc.update(DELETE_SQL, idsSetter(one))
        metrics.movedToDead(target)
        log.error("{} outbox {} → history_outbox_dead: {}", target, id, reason)
    }

    /** 맨 안쪽 원인만 — 바깥 예외 메시지는 SQL 전체가 붙어 길다 */
    private fun rootMessage(e: Exception): String {
        val root = generateSequence<Throwable>(e) { it.cause }.last()
        return root.message ?: root.javaClass.name
    }

    /** 권한 없음·버킷 없음 — 다시 해도 안 되고 사람이 고쳐야 하는 설정 오류 */
    private fun isConfigError(e: Exception): Boolean =
        generateSequence<Throwable>(e) { it.cause }.filterIsInstance<S3Exception>().any { it.statusCode() in CONFIG_ERROR_STATUS }

    /**
     * 전환 기간 이중 쓰기 — 스냅샷(payload.payload)에 변경 정보·변경 시각·event_id 를 붙여 예전 이력 테이블 한 행으로 펼친다.
     * 칸은 이력 테이블에서 읽는다(자동 번호 빼고). event_id 가 같으면 넣지 않는다(재시도 중복).
     */
    private fun tableInsertSql(target: HistoryTarget): String = tableInsertSql.getOrPut(target) {
        val columns = jdbc.queryForList(COLUMNS_SQL, String::class.java, target.historyTable)
        check(EVENT_ID in columns) { "${target.historyTable} 에 $EVENT_ID 칸이 없다 — snsb3_api V12 를 먼저 적용해야 한다" }
        """
        INSERT INTO ${target.historyTable} (${columns.joinToString(", ") { "\"$it\"" }})
        SELECT ${columns.joinToString(", ") { "r.\"$it\"" }}
          FROM history_outbox o
         CROSS JOIN LATERAL jsonb_populate_record(
               NULL::${target.historyTable},
               (o.payload -> 'payload')
                 || jsonb_build_object('change_type', o.payload -> 'change_type', 'changed_by', o.payload -> 'changed_by',
                                       'change_reason', o.payload -> 'change_reason', 'device_info', o.payload -> 'device_info',
                                       'app_version', o.payload -> 'app_version', 'changed_at', o.occurred_at, '$EVENT_ID', o.event_id)
         ) r
         WHERE o.id = ANY(?)
         ORDER BY o.occurred_at, o.id
        ON CONFLICT ($EVENT_ID) DO NOTHING
        """.trimIndent()
    }

    private fun idFileName(ids: List<Long>) = "${ids.first()}-${ids.last()}"

    companion object {
        private const val FETCH_SIZE = 200
        private const val ID = "id"
        private const val OCCURRED_AT = "occurred_at"
        private const val LINE = "line"
        private const val BYTES = "bytes"
        private const val WELL_FORMED = "well_formed"
        private const val EVENT_ID = "event_id"
        private const val BIGINT = "bigint"
        private const val MALFORMED = "형식이 깨진 행 — 필수 칸(change_type, payload 객체)이 없다"
        /** 403 권한 없음, 404 버킷 없음 */
        private val CONFIG_ERROR_STATUS = setOf(403, 404)

        /** 한 줄 = payload(조회 키·변경 정보·스냅샷) + event_id·schema_version·history_type·changed_at */
        private const val LOCK_SQL = """
            SELECT id, occurred_at, line, octet_length(line) AS bytes, well_formed
              FROM (SELECT id, occurred_at,
                           (payload || jsonb_build_object('event_id', event_id, 'schema_version', ?::int,
                                                          'history_type', ?::text, 'changed_at', occurred_at))::text AS line,
                           ((payload -> 'change_type') IS NOT NULL AND jsonb_typeof(payload -> 'payload') = 'object') AS well_formed
                      FROM history_outbox
                     WHERE history_type = ?
                     ORDER BY occurred_at, id
                     LIMIT ?
                       FOR UPDATE SKIP LOCKED) locked
             ORDER BY occurred_at, id
        """
        private const val DUE_SQL = """
            SELECT coalesce(count(*) >= ?, false)
                OR coalesce(min(occurred_at) <= clock_timestamp() - ? * interval '1 millisecond', false)
              FROM (SELECT occurred_at FROM history_outbox WHERE history_type = ? ORDER BY occurred_at, id LIMIT ?) head
        """
        private const val DELETE_SQL = "DELETE FROM history_outbox WHERE id = ANY(?)"
        private const val DEAD_SQL = """
            INSERT INTO history_outbox_dead (id, event_id, history_type, occurred_at, payload, last_error)
            SELECT id, event_id, history_type, occurred_at, payload, ? FROM history_outbox WHERE id = ANY(?)
        """
        private const val COLUMNS_SQL = """
            SELECT column_name FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = ?
               AND is_identity = 'NO' AND is_generated = 'NEVER'
             ORDER BY ordinal_position
        """

        fun idsSetter(ids: List<Long>) = PreparedStatementSetter { ps ->
            ps.setArray(1, ps.connection.createArrayOf(BIGINT, ids.toTypedArray()))
        }
    }
}

/** S3 업로드 — gzip JSON Lines 파일 하나 */
@Component
class HistoryUploader(private val s3: S3Client, private val properties: S3Properties) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun put(target: HistoryTarget, partition: String, fileName: String, gzipped: ByteArray, lines: Int) {
        val key = objectKey(properties.prefix, target, partition, fileName)
        s3.putObject(
            PutObjectRequest.builder().bucket(properties.bucket).key(key)
                .contentType(CONTENT_TYPE).contentEncoding(CONTENT_ENCODING).build(),
            RequestBody.fromBytes(gzipped),
        )
        log.info("{} 이력 {}건 → s3://{}/{} ({}바이트)", target, lines, properties.bucket, key, gzipped.size)
    }

    private companion object {
        const val CONTENT_TYPE = "application/x-ndjson"
        const val CONTENT_ENCODING = "gzip"
    }
}

/**
 * 한 묶음의 줄을 시간 칸(한국 시간 dt/hour)별 gzip 파일로 모은다 — 묶음이 시간 경계를 넘으면 칸마다 파일 하나.
 * 파일 이름은 칸 안의 첫·끝 id(outbox id, 백필은 history_id).
 */
class HourlyJsonLines {
    private class File(val bytes: ByteArrayOutputStream, val writer: BufferedWriter, val ids: MutableList<Long>)

    private val files = linkedMapOf<String, File>()

    fun add(at: OffsetDateTime, id: Long, line: String) {
        val file = files.getOrPut(hourPartition(at)) {
            val bytes = ByteArrayOutputStream()
            File(bytes, GZIPOutputStream(bytes).bufferedWriter(), mutableListOf())
        }
        file.writer.write(line)
        file.writer.newLine()
        file.ids.add(id)
    }

    fun isEmpty() = files.isEmpty()

    fun ids(): List<Long> = files.values.flatMap { it.ids }

    fun upload(uploader: HistoryUploader, target: HistoryTarget, fileName: (List<Long>) -> String) {
        files.forEach { (partition, file) ->
            file.writer.close()
            uploader.put(target, partition, fileName(file.ids), file.bytes.toByteArray(), file.ids.size)
        }
    }
}
