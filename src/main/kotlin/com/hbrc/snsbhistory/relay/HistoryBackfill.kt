package com.hbrc.snsbhistory.relay

import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.OffsetDateTime

/**
 * 기존 이력 테이블 → S3 한 번 올리기(docs/history-outbox-s3-plan.md 8절 1단계). backfill.enabled=true 일 때만 돌고 끝나면 앱을 내린다.
 *
 * - 대상: event_id 가 없는 행(아웃박스 이전 이력). event_id 가 있는 행은 이중 쓰기를 하던 때 relay 가 이미 S3 에 올렸다
 * - event_id: md5('{테이블}:{history_id}') 로 만든 UUID — 다시 돌려도 같은 값이라 읽을 때 중복이 걸러진다
 * - 메모리: 줄 크기를 DB 에서 먼저 재서 합계 [BackfillProperties.maxBatchBytes] 까지만 본문을 읽어 올린다. 한 건이 그보다 크면 그 한 건만
 * - 형식·경로는 relay 와 같다. 파일 이름은 h{첫 history_id}-h{끝 history_id} (outbox id 와 겹치지 않게)
 * - 소검사 이력의 examinee_id 는 회차(test)에서 찾는다
 * - 끊겨도 처음부터 다시 돌리면 된다 — 같은 행은 같은 파일 이름·같은 event_id 로 다시 올라간다
 */
@Component
@ConditionalOnProperty(name = ["backfill.enabled"], havingValue = "true")
class HistoryBackfill(
    private val jdbc: JdbcTemplate,
    private val uploader: HistoryUploader,
    private val properties: BackfillProperties,
    private val context: ConfigurableApplicationContext,
    transactionManager: PlatformTransactionManager,
) : ApplicationRunner {
    private val log = LoggerFactory.getLogger(javaClass)
    /** 자동 커밋이면 드라이버가 fetchSize 를 무시하고 한 번에 다 받는다 — 커서로 조금씩 읽으려고 트랜잭션 안에서 */
    private val readOnly = TransactionTemplate(transactionManager).apply { isReadOnly = true }

    override fun run(args: ApplicationArguments) {
        HistoryTarget.entries.filter { it.hasHistoryTable }.forEach { target ->
            var after = 0L
            var total = 0
            while (true) {
                val picked = pick(target, after)
                if (picked.isEmpty()) break
                val files = HourlyJsonLines()
                jdbc.query(
                    { con ->
                        con.prepareStatement(linesSqlOf(target)).apply {
                            fetchSize = FETCH_SIZE
                            setInt(1, SCHEMA_VERSION)
                            setString(2, target.typeName)
                            setArray(3, con.createArrayOf(BIGINT, picked.toTypedArray()))
                        }
                    },
                    RowCallbackHandler { rs ->
                        files.add(rs.getObject(CHANGED_AT, OffsetDateTime::class.java), rs.getLong(HISTORY_ID), rs.getString(LINE))
                    },
                )
                files.upload(uploader, target) { ids -> "h${ids.first()}-h${ids.last()}" }
                total += picked.size
                after = picked.last()
            }
            log.info("{} 백필 끝 — {}건", target.historyTable, total)
        }
        context.close()
    }

    /**
     * [after] 다음부터 history_id 순으로 최대 batchSize 건, 줄 크기 합계 [BackfillProperties.maxBatchBytes] 까지 고른다(첫 줄은 혼자라도).
     * 줄 본문은 DB 에서 재기만 하고 가져오지 않는다 — 커서로 조금씩 읽어 멈춘 뒤의 행은 재지 않는다
     */
    private fun pick(target: HistoryTarget, after: Long): List<Long> {
        val picked = mutableListOf<Long>()
        var bytes = 0L
        readOnly.executeWithoutResult {
            jdbc.query(
                { con ->
                    con.prepareStatement(sizesSqlOf(target)).apply {
                        fetchSize = SIZE_FETCH_SIZE
                        setInt(1, SCHEMA_VERSION)
                        setString(2, target.typeName)
                        setLong(3, after)
                        setInt(4, properties.batchSize)
                    }
                },
                ResultSetExtractor { rs ->
                    while (rs.next()) {
                        val size = rs.getLong(BYTES)
                        if (picked.isNotEmpty() && bytes + size > properties.maxBatchBytes) break
                        bytes += size
                        picked.add(rs.getLong(HISTORY_ID))
                    }
                },
            )
        }
        return picked
    }

    /** 고를 행의 줄 크기 — 자리표: schema_version, history_type, 이 history_id 다음부터, 최대 건수 */
    private fun sizesSqlOf(target: HistoryTarget): String = """
        SELECT h.history_id, octet_length(${lineSqlOf(target)}) AS bytes
          FROM ${target.historyTable} h ${joinOf(target)}
         WHERE h.event_id IS NULL AND h.history_id > ?
         ORDER BY h.history_id
         LIMIT ?
    """.trimIndent()

    /** 고른 행의 줄 본문 — 자리표: schema_version, history_type, history_id 배열 */
    private fun linesSqlOf(target: HistoryTarget): String = """
        SELECT h.history_id, coalesce(h.changed_at, h.updated_at, h.created_at) AS changed_at, ${lineSqlOf(target)} AS line
          FROM ${target.historyTable} h ${joinOf(target)}
         WHERE h.history_id = ANY(?)
         ORDER BY h.history_id
    """.trimIndent()

    /** 이력 테이블 한 행 → S3 한 줄(relay 와 같은 형식). changed_at 이 없던 행은 수정·생성 시각으로 */
    private fun lineSqlOf(target: HistoryTarget): String {
        val (examineeId, testId, subId) = keysOf(target)
        return """
            jsonb_build_object(
                'event_id', md5('${target.historyTable}:' || h.history_id)::uuid,
                'schema_version', ?::int, 'history_type', ?::text,
                'change_type', h.change_type, 'changed_at', coalesce(h.changed_at, h.updated_at, h.created_at),
                'changed_by', h.changed_by, 'change_reason', h.change_reason,
                'device_info', h.device_info, 'app_version', h.app_version,
                'examinee_id', $examineeId, 'test_id', $testId, 'sub_id', $subId,
                'payload', to_jsonb(h) - ARRAY[$META_COLUMNS]
            )::text
        """.trimIndent()
    }

    private fun joinOf(target: HistoryTarget): String = keysOf(target).join

    private fun keysOf(target: HistoryTarget): Keys = when (target) {
        HistoryTarget.TEST_SUB -> Keys("t.examinee_id", "h.test_id", "h.id", "LEFT JOIN test t ON t.id = h.test_id")
        HistoryTarget.EXAMINEE -> Keys("h.id", "NULL", "NULL", "")
        HistoryTarget.STAFF -> error("직원 이력은 예전 테이블이 없어 백필하지 않는다")
        HistoryTarget.TEST -> error("검사 이력은 예전 테이블이 없어 백필하지 않는다")
    }

    private data class Keys(val examineeId: String, val testId: String, val subId: String, val join: String)

    private companion object {
        const val FETCH_SIZE = 500
        /** 크기를 볼 때는 조금씩 읽는다 — 크기에서 멈출 때 그 뒤로 미리 재는 행이 이만큼을 넘지 않게 */
        const val SIZE_FETCH_SIZE = 50
        const val BYTES = "bytes"
        const val BIGINT = "bigint"
        const val HISTORY_ID = "history_id"
        const val CHANGED_AT = "changed_at"
        const val LINE = "line"
        /** 스냅샷(payload)에서 빼는 칸 — 변경 정보는 최상위에 있고, 자동 번호·검색용 칸은 스냅샷이 아니다 */
        const val META_COLUMNS = "'history_id', 'event_id', 'change_type', 'changed_by', 'changed_at', 'change_reason', " +
            "'device_info', 'app_version', 'search_vector'"
    }
}
