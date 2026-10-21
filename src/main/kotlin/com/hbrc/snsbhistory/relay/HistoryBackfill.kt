package com.hbrc.snsbhistory.relay

import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.stereotype.Component
import java.time.OffsetDateTime

/**
 * 기존 이력 테이블 → S3 한 번 올리기(docs/history-outbox-s3-plan.md 8절 1단계). backfill.enabled=true 일 때만 돌고 끝나면 앱을 내린다.
 *
 * - 대상: event_id 가 없는 행(아웃박스 이전 이력). event_id 가 있는 행은 이중 쓰기로 relay 가 이미 S3 에 올렸다
 * - event_id: md5('{테이블}:{history_id}') 로 만든 UUID — 다시 돌려도 같은 값이라 읽을 때 중복이 걸러진다
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
) : ApplicationRunner {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun run(args: ApplicationArguments) {
        HistoryTarget.entries.filter { it.hasHistoryTable }.forEach { target ->
            var after = 0L
            var total = 0
            while (true) {
                val files = HourlyJsonLines()
                var last = after
                var read = 0
                jdbc.query(
                    { con ->
                        con.prepareStatement(sqlOf(target)).apply {
                            fetchSize = FETCH_SIZE
                            setInt(1, SCHEMA_VERSION)
                            setString(2, target.typeName)
                            setLong(3, after)
                            setInt(4, properties.batchSize)
                        }
                    },
                    RowCallbackHandler { rs ->
                        read++
                        last = rs.getLong(HISTORY_ID)
                        files.add(rs.getObject(CHANGED_AT, OffsetDateTime::class.java), last, rs.getString(LINE))
                    },
                )
                if (read == 0) break
                files.upload(uploader, target) { ids -> "h${ids.first()}-h${ids.last()}" }
                total += read
                after = last
            }
            log.info("{} 백필 끝 — {}건", target.historyTable, total)
        }
        context.close()
    }

    /** 이력 테이블 한 행 → S3 한 줄(relay 와 같은 형식). changed_at 이 없던 행은 수정·생성 시각으로 */
    private fun sqlOf(target: HistoryTarget): String {
        val (examineeId, testId, subId, join) = when (target) {
            HistoryTarget.TEST_SUB -> Keys("t.examinee_id", "h.test_id", "h.id", "LEFT JOIN test t ON t.id = h.test_id")
            HistoryTarget.EXAMINEE -> Keys("h.id", "NULL", "NULL", "")
            HistoryTarget.STAFF -> error("직원 이력은 예전 테이블이 없어 백필하지 않는다")
        }
        return """
            SELECT h.history_id, coalesce(h.changed_at, h.updated_at, h.created_at) AS changed_at,
                   jsonb_build_object(
                       'event_id', md5('${target.historyTable}:' || h.history_id)::uuid,
                       'schema_version', ?::int, 'history_type', ?::text,
                       'change_type', h.change_type, 'changed_at', coalesce(h.changed_at, h.updated_at, h.created_at),
                       'changed_by', h.changed_by, 'change_reason', h.change_reason,
                       'device_info', h.device_info, 'app_version', h.app_version,
                       'examinee_id', $examineeId, 'test_id', $testId, 'sub_id', $subId,
                       'payload', to_jsonb(h) - ARRAY[$META_COLUMNS]
                   )::text AS line
              FROM ${target.historyTable} h $join
             WHERE h.event_id IS NULL AND h.history_id > ?
             ORDER BY h.history_id
             LIMIT ?
        """.trimIndent()
    }

    private data class Keys(val examineeId: String, val testId: String, val subId: String, val join: String)

    private companion object {
        const val FETCH_SIZE = 500
        const val HISTORY_ID = "history_id"
        const val CHANGED_AT = "changed_at"
        const val LINE = "line"
        /** 스냅샷(payload)에서 빼는 칸 — 변경 정보는 최상위에 있고, 자동 번호·검색용 칸은 스냅샷이 아니다 */
        const val META_COLUMNS = "'history_id', 'event_id', 'change_type', 'changed_by', 'changed_at', 'change_reason', " +
            "'device_info', 'app_version', 'search_vector'"
    }
}
