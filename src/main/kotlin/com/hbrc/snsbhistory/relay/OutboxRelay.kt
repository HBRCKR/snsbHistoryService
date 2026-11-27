package com.hbrc.snsbhistory.relay

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.event.ContextClosedEvent
import org.springframework.context.event.EventListener
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.jdbc.core.ResultSetExtractor
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.time.OffsetDateTime
import java.util.EnumMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPOutputStream

/**
 * history_outbox → S3 (docs/history-outbox-s3-plan.md 5절, docs/history-service.md).
 *
 * 아웃박스에 [RelayProperties.batchSize] 건이 쌓였거나 가장 오래된 건이 [RelayProperties.flushIntervalMs] 를 기다렸으면,
 * 종류를 가리지 않고 앞에서부터(id 순) 최대 batchSize 건, 합계 [RelayProperties.maxBatchBytes] 까지를 한 묶음으로 잠근다.
 * 한 줄 = 이력 한 건(JSON). 묶음을 (이력 종류, 시간 칸)마다 gzip 파일 하나로 S3 에 올린 뒤 같은 트랜잭션에서 행을 지운다.
 * - 한 줄로 가져오니 먼저 들어온 이력이 먼저 나간다 — 한 종류가 많이 쌓여도 다른 종류가 뒤로 밀려 굶지 않는다
 * - [HistoryTarget] 에 없는 종류는 가져가지 않는다(이 서비스가 아직 모르는 종류 — 그 종류를 넣어 배포하면 나간다)
 *
 * 메모리 — 줄 본문은 묶음에 담을 행만 읽는다:
 * 1. 잠그기: `FOR UPDATE SKIP LOCKED` 로 앞에서부터 줄 크기·형식만 읽으며 잠근다. 합계가 maxBatchBytes 를 넘는 행에서 멈춘다
 *    — 그 행부터는 다음 묶음(순서 유지). 커서로 조금씩 읽으니 멈춘 뒤의 행은 잠그지도 크기를 재지도 않는다
 * 2. 읽기: 고른 행의 줄 본문만 읽는다 — 한 번에 올리는 크기가 maxBatchBytes 를 넘지 않는다
 *
 * 실패 처리 — 보낼 수 없는 행은 보내기 전에 걸러 dead 로, 업로드 실패는 묶음째 장애로:
 * - 보내기 전 검사([DeadReason]): 형식이 깨진 행(change_type 없음, payload 가 객체가 아님)과 한 건이 maxBatchBytes 보다 큰 행은
 *   묶음에 넣지 않고 history_outbox_dead 로. 다시 해도 같다(큰 행은 relay 메모리를 지키려고 — 실제 최대 rawData 는 수 MB).
 *   dead 이동은 업로드와 따로 커밋한다 — 업로드가 실패해 묶음이 롤백돼도 치워지고, 재시도마다 다시 세지 않는다
 * - 업로드 실패: 묶음 전체를 롤백해 아웃박스에 그대로 두고 backoff 뒤 같은 묶음을 다시 올린다. 한 건씩 나눠 쓰지 않는다 —
 *   S3 파일 이름은 종류·시각·id 로만 정해지고 PUT 은 통째로 되거나 안 되므로, 검사를 통과한 행 탓에 업로드가 실패하는 일은 없다.
 *   그래도 계속 실패하면 앞에서 멈춘다(건너뛰지 않음) — oldest age 지표로 드러난다
 * - 권한·버킷 없음 같은 설정 오류는 지표를 따로 올린다(사람이 고쳐야 한다)
 * - S3 호출은 시간 상한이 있다([S3Properties.callTimeoutMs]) — 느린 S3 때문에 트랜잭션·행 잠금을 오래 쥐지 않는다
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
    private val stopping = AtomicBoolean(false)
    private var backoffMs = relay.backoffInitialMs
    private var retryAtMs = 0L

    /** 묶음에 담는 한 줄 */
    private class Line(val id: Long, val target: HistoryTarget, val occurredAt: OffsetDateTime, val text: String)

    @EventListener(ContextClosedEvent::class)
    fun stop() = stopping.set(true)

    @Scheduled(fixedDelayString = "\${relay.poll-interval-ms:10000}")
    fun poll() {
        if (stopping.get() || System.currentTimeMillis() < retryAtMs) return
        try {
            // 올릴 때가 된 동안 이어서 비운다(가득 찼거나 오래 기다린 이력이 남아 있는 동안)
            while (!stopping.get() && isDue() && relayBatch() > 0) Unit
            backoffMs = relay.backoffInitialMs
        } catch (e: Exception) {
            if (generateSequence<Throwable>(e) { it.cause }.any { it is SdkException }) metrics.s3Failed()
            retryAtMs = System.currentTimeMillis() + backoffMs
            log.warn("이력 업로드 실패(장애) — {}ms 뒤 다시 시도한다: {}", backoffMs, e.message)
            backoffMs = minOf(backoffMs * 2, relay.backoffMaxMs)
        }
    }

    /** 올릴 때가 됐나 — 앞에서 batchSize 건만 보고, 가득 찼거나 가장 오래된 건이 flushIntervalMs 이상 기다렸으면 참 */
    private fun isDue(): Boolean =
        jdbc.queryForObject(DUE_SQL, Boolean::class.java, relay.batchSize, relay.flushIntervalMs, relay.batchSize) == true

    /**
     * 한 묶음을 올리고 지운다. 잠근 행 수를 돌려준다(0 이면 더 없음). 업로드가 실패하면 묶음 전체가 롤백되고 예외가 그대로 나간다.
     * 보낼 수 없는 행은 그 뒤 따로 커밋해 dead 로 옮긴다 — 업로드 성패와 상관없이
     */
    fun relayBatch(): Int {
        val rejected = EnumMap<DeadReason, MutableList<Long>>(DeadReason::class.java)
        var failure: Exception? = null
        val locked = try {
            transaction.execute { lockAndDeliver(rejected) } ?: 0
        } catch (e: Exception) {
            failure = e
            0
        }
        rejected.forEach { (reason, ids) ->
            try {
                moveToDead(ids, reason)
            } catch (e: Exception) {
                val first = failure
                if (first == null) failure = e else first.addSuppressed(e)
            }
        }
        failure?.let { throw it }
        return locked
    }

    /** 앞에서부터 잠그고, 보낼 수 없는 행은 [rejected] 에 사유별로 모아 묶음에서 뺀 뒤 나머지를 올린다 */
    private fun lockAndDeliver(rejected: MutableMap<DeadReason, MutableList<Long>>): Int {
        val picked = mutableListOf<Long>()
        var bytes = 0L
        var locked = 0
        jdbc.query(
            { con ->
                con.prepareStatement(LOCK_SQL).apply {
                    fetchSize = LOCK_FETCH_SIZE
                    setInt(1, SCHEMA_VERSION)
                    setInt(2, relay.batchSize)
                }
            },
            ResultSetExtractor { rs ->
                while (rs.next()) {
                    locked++
                    val id = rs.getLong(ID)
                    val size = rs.getLong(BYTES)
                    val reason = when {
                        !rs.getBoolean(WELL_FORMED) -> DeadReason.MALFORMED
                        size > relay.maxBatchBytes -> DeadReason.TOO_LARGE
                        else -> null
                    }
                    if (reason != null) {
                        rejected.getOrPut(reason) { mutableListOf() }.add(id)
                        continue
                    }
                    // 합계가 상한을 넘는 행에서 멈춘다 — 그 행부터는 다음 묶음(순서 유지)
                    if (bytes + size > relay.maxBatchBytes) break
                    bytes += size
                    picked.add(id)
                }
            },
        )
        // 보내기 전 검사 — 보낼 수 없는 행은 묶음에 넣지 않는다(dead 이동은 relayBatch 가 따로 커밋)
        if (picked.isNotEmpty()) deliver(readLines(picked))
        return locked
    }

    /** 잠근 행 중 묶음에 담을 행의 줄 본문만 읽는다(id 순) */
    private fun readLines(ids: List<Long>): List<Line> {
        val lines = ArrayList<Line>(ids.size)
        jdbc.query(
            { con ->
                con.prepareStatement(LINES_SQL).apply {
                    fetchSize = FETCH_SIZE
                    setInt(1, SCHEMA_VERSION)
                    setArray(2, con.createArrayOf(BIGINT, ids.toTypedArray()))
                }
            },
            RowCallbackHandler { rs ->
                lines.add(
                    Line(
                        rs.getLong(ID), HistoryTarget.valueOf(rs.getString(HISTORY_TYPE)),
                        rs.getObject(OCCURRED_AT, OffsetDateTime::class.java), rs.getString(LINE),
                    ),
                )
            },
        )
        return lines
    }

    /**
     * S3 업로드((종류, 시간 칸)마다 파일 하나) → 아웃박스에서 지우기.
     * 업로드가 실패하면 예외 — 묶음 전체가 롤백돼 아웃박스에 그대로 남는다
     */
    private fun deliver(lines: List<Line>) {
        val byTarget = lines.groupBy { it.target }
        try {
            byTarget.forEach { (target, targetLines) ->
                val files = HourlyJsonLines()
                targetLines.forEach { files.add(it.occurredAt, it.id, it.text) }
                files.upload(uploader, target, ::idFileName)
            }
        } catch (e: Exception) {
            if (isConfigError(e)) {
                metrics.s3ConfigError()
                log.error("이력 업로드 — S3 설정 오류(권한·버킷)라 사람이 고쳐야 한다: {}", e.message)
            }
            throw e
        }
        jdbc.update(DELETE_SQL, idsSetter(lines.map { it.id }))
        byTarget.forEach { (target, targetLines) -> metrics.relayed(target, targetLines.size) }
    }

    /**
     * 보낼 수 없는 행 — 다른 이력을 막지 않게 dead 로 치워 둔다(원인을 고친 뒤 outbox 로 되돌린다).
     * 문장 하나로 옮기고 지워 그 자체로 커밋된다. 다른 인스턴스가 잡고 있는 행은 건너뛴다(그쪽이 옮긴다). 옮긴 건수만 센다
     */
    private fun moveToDead(ids: List<Long>, reason: DeadReason) {
        val moved = jdbc.query(
            MOVE_TO_DEAD_SQL,
            PreparedStatementSetter { ps ->
                ps.setArray(1, ps.connection.createArrayOf(BIGINT, ids.toTypedArray()))
                ps.setString(2, reason.message)
            },
        ) { rs, _ -> HistoryTarget.valueOf(rs.getString(HISTORY_TYPE)) }
        if (moved.isEmpty()) return
        moved.groupingBy { it }.eachCount().forEach { (target, count) -> metrics.movedToDead(target, count) }
        log.error("outbox {}건 → history_outbox_dead: {} (id {})", moved.size, reason.message, ids)
    }

    /** 권한 없음·버킷 없음 — 다시 해도 안 되고 사람이 고쳐야 하는 설정 오류 */
    private fun isConfigError(e: Exception): Boolean =
        generateSequence<Throwable>(e) { it.cause }.filterIsInstance<S3Exception>().any { it.statusCode() in CONFIG_ERROR_STATUS }

    private fun idFileName(ids: List<Long>) = "${ids.first()}-${ids.last()}"

    companion object {
        private const val FETCH_SIZE = 200
        /** 잠그기 단계는 조금씩 읽는다 — 크기에서 멈출 때 그 뒤로 미리 잠그고 크기를 재는 행이 이만큼을 넘지 않게 */
        private const val LOCK_FETCH_SIZE = 50
        private const val ID = "id"
        private const val HISTORY_TYPE = "history_type"
        private const val OCCURRED_AT = "occurred_at"
        private const val LINE = "line"
        private const val BYTES = "bytes"
        private const val WELL_FORMED = "well_formed"
        private const val BIGINT = "bigint"
        /** 403 권한 없음, 404 버킷 없음 */
        private val CONFIG_ERROR_STATUS = setOf(403, 404)

        /** 이 서비스가 아는 종류만 가져간다 — 아웃박스 값(enum 이름, 예: TEST_SUB) */
        private val KNOWN_TYPES = HistoryTarget.entries.joinToString(", ") { "'${it.name}'" }

        /** 아웃박스 값(TEST_SUB) → 줄의 history_type(test_sub) */
        private val TYPE_NAME_SQL = HistoryTarget.entries.joinToString(" ", "CASE history_type ", " END") {
            "WHEN '${it.name}' THEN '${it.typeName}'"
        }

        /** 한 줄 = payload(조회 키·변경 정보·스냅샷) + event_id·schema_version·history_type·changed_at. 자리표: schema_version */
        private val LINE_SQL = """
            (payload || jsonb_build_object('event_id', event_id, 'schema_version', ?::int,
                                           'history_type', $TYPE_NAME_SQL, 'changed_at', occurred_at))::text
        """

        /**
         * 줄 크기·형식만 보며 앞에서부터(id 순, 기본 키 색인) 잠근다 — 본문은 DB 에서 재기만 하고 가져오지 않는다.
         * 커서로 읽은 만큼만 잠그고 잰다. 자리표: schema_version, batchSize
         */
        private val LOCK_SQL = """
            SELECT id, octet_length($LINE_SQL) AS bytes,
                   ((payload -> 'change_type') IS NOT NULL AND jsonb_typeof(payload -> 'payload') = 'object') AS well_formed
              FROM history_outbox
             WHERE history_type IN ($KNOWN_TYPES)
             ORDER BY id
             LIMIT ?
               FOR UPDATE SKIP LOCKED
        """

        /** 이미 잠근 행의 종류·시각·줄 본문. 자리표: schema_version, id 배열 */
        private val LINES_SQL = """
            SELECT id, history_type, occurred_at, $LINE_SQL AS line
              FROM history_outbox
             WHERE id = ANY(?)
             ORDER BY id
        """

        /** 자리표: batchSize, flushIntervalMs, batchSize */
        private val DUE_SQL = """
            SELECT coalesce(count(*) >= ?, false)
                OR coalesce(min(occurred_at) <= clock_timestamp() - ? * interval '1 millisecond', false)
              FROM (SELECT occurred_at FROM history_outbox WHERE history_type IN ($KNOWN_TYPES) ORDER BY id LIMIT ?) head
        """
        private const val DELETE_SQL = "DELETE FROM history_outbox WHERE id = ANY(?)"

        /** 자리표: id 배열, 사유. 옮긴 행의 종류를 돌려준다 */
        private const val MOVE_TO_DEAD_SQL = """
            WITH moved AS (
                DELETE FROM history_outbox
                 WHERE id IN (SELECT id FROM history_outbox WHERE id = ANY(?) FOR UPDATE SKIP LOCKED)
                RETURNING id, event_id, history_type, occurred_at, payload
            )
            INSERT INTO history_outbox_dead (id, event_id, history_type, occurred_at, payload, last_error)
            SELECT id, event_id, history_type, occurred_at, payload, ? FROM moved
            RETURNING history_type
        """

        fun idsSetter(ids: List<Long>) = PreparedStatementSetter { ps ->
            ps.setArray(1, ps.connection.createArrayOf(BIGINT, ids.toTypedArray()))
        }
    }
}

/** 보내기 전 검사에서 걸러 dead 로 보내는 사유 — history_outbox_dead.last_error 에 [message] 가 남는다 */
enum class DeadReason(val message: String) {
    MALFORMED("형식이 깨진 행 — 필수 칸(change_type, payload 객체)이 없다"),
    TOO_LARGE("한 건이 묶음 상한(relay.max-batch-bytes)보다 크다 — relay 메모리를 지키려고 보내지 않는다"),
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
