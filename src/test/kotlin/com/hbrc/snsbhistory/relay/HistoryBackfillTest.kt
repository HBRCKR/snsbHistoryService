package com.hbrc.snsbhistory.relay

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.springframework.boot.DefaultApplicationArguments
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import software.amazon.awssdk.services.s3.S3Client
import java.util.UUID
import java.util.zip.GZIPInputStream
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DB_URL_ENV = "HISTORY_TEST_DB_URL"

/**
 * 기존 이력 테이블 → S3 백필 — 로컬 PostgreSQL 에 붙어 돈다. [DB_URL_ENV] 가 있을 때만(OutboxRelayTest 와 같은 값).
 * 테스트 전용 스키마([SCHEMA])를 테스트마다 새로 만든다. S3 는 올린 파일을 기억하는 가짜 업로더. 테스트 JVM 힙은 256MB.
 */
@EnabledIfEnvironmentVariable(named = DB_URL_ENV, matches = ".+")
class HistoryBackfillTest {
    /** 가짜 업로더에 올라간 파일 하나 — 파일 이름, 줄(순서대로), 줄 크기 합계(줄바꿈 빼고) */
    private class Upload(val target: HistoryTarget, val fileName: String, val lines: List<Line>, val bytes: Long)

    private class Line(val eventId: UUID, val examineeId: String)

    private class RecordingUploader : HistoryUploader(mock(S3Client::class.java), S3Properties(BUCKET)) {
        val uploads = mutableListOf<Upload>()

        override fun put(target: HistoryTarget, partition: String, fileName: String, gzipped: ByteArray, lines: Int) {
            val texts = GZIPInputStream(gzipped.inputStream()).bufferedReader().readLines()
            val parsed = texts.map { JSON.readTree(it) }.map { Line(UUID.fromString(it.get(EVENT_ID).asText()), it.get(EXAMINEE_ID).asText()) }
            uploads.add(Upload(target, fileName, parsed, texts.sumOf { it.toByteArray().size.toLong() }))
        }

        fun of(target: HistoryTarget): List<Upload> = uploads.filter { it.target == target }
    }

    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = System.getenv(DB_URL_ENV)
        schema = SCHEMA
        maximumPoolSize = POOL_SIZE
        connectionInitSql = TIME_ZONE_SQL
    })
    private val jdbc = JdbcTemplate(dataSource)

    @BeforeEach
    fun createSchema() {
        jdbc.execute(DROP_SCHEMA_SQL)
        jdbc.execute(CREATE_SCHEMA_SQL)
    }

    @AfterEach
    fun dropSchema() {
        jdbc.execute(DROP_SCHEMA_SQL)
        dataSource.close()
    }

    @Test
    fun `큰 rawData 가 몰려도 읽는 크기를 상한까지로 잘라 순서대로 모두 올린다`() {
        // 절반이 2.4MB 짜리(합계 약 600MB) — 256MB 힙에서 한 번에 읽으면 OOM
        insertTestSubRows(BIG_ROWS) { if (it % 2 == 0) BIG_CHARS else SMALL_CHARS }
        insertExamineeRows(3)
        val uploader = RecordingUploader()
        val context = mock(ConfigurableApplicationContext::class.java)

        backfill(uploader, context, DEFAULT_MAX_BATCH_BYTES).run(DefaultApplicationArguments())

        val uploads = uploader.of(HistoryTarget.TEST_SUB)
        assertEquals(eventIds(TEST_SUB_HISTORY), uploads.flatMap { it.lines }.map { it.eventId })
        uploads.filter { it.lines.size > 1 }.forEach {
            assertTrue(it.bytes <= DEFAULT_MAX_BATCH_BYTES, "여러 줄 파일이 상한을 넘었다: ${it.bytes}")
        }
        assertTrue(uploads.flatMap { it.lines }.all { it.examineeId == EXAMINEE }, "소검사 이력의 examinee_id 는 회차에서 찾는다")
        assertEquals(eventIds(EXAMINEE_HISTORY), uploader.of(HistoryTarget.EXAMINEE).flatMap { it.lines }.map { it.eventId })
        verify(context).close()
    }

    @Test
    fun `상한보다 큰 한 행은 그 한 행만 파일 하나로 올리고 순서를 건너뛰지 않는다`() {
        insertTestSubRows(4) { if (it == 2) BIG_CHARS else SMALL_CHARS }
        val uploader = RecordingUploader()

        backfill(uploader, mock(ConfigurableApplicationContext::class.java), SMALL_MAX_BATCH_BYTES).run(DefaultApplicationArguments())

        assertEquals(listOf("h1-h1", "h2-h2", "h3-h4"), uploader.of(HistoryTarget.TEST_SUB).map { it.fileName })
    }

    @Test
    fun `event_id 가 있는 행은 relay 가 이미 올렸으니 건너뛴다`() {
        insertTestSubRows(3) { SMALL_CHARS }
        jdbc.update(MARK_RELAYED_SQL)
        val uploader = RecordingUploader()

        backfill(uploader, mock(ConfigurableApplicationContext::class.java), DEFAULT_MAX_BATCH_BYTES).run(DefaultApplicationArguments())

        val uploads = uploader.of(HistoryTarget.TEST_SUB)
        assertEquals(listOf("h1-h3"), uploads.map { it.fileName })
        assertEquals(eventIds(TEST_SUB_HISTORY), uploads.flatMap { it.lines }.map { it.eventId })
        assertEquals(2, uploads.single().lines.size)
    }

    private fun backfill(uploader: HistoryUploader, context: ConfigurableApplicationContext, maxBatchBytes: Long) = HistoryBackfill(
        jdbc,
        uploader,
        BackfillProperties(enabled = true, maxBatchBytes = maxBatchBytes),
        context,
        DataSourceTransactionManager(dataSource),
    )

    /** n 번째(1부터) 행의 raw_data 는 [chars] (n) 글자 '가'(3바이트) — 본문은 DB 에서 만든다 */
    private fun insertTestSubRows(count: Int, chars: (Int) -> Int) {
        (1..count).forEach { n -> jdbc.update(INSERT_TEST_SUB_SQL, n, chars(n)) }
    }

    private fun insertExamineeRows(count: Int) {
        repeat(count) { jdbc.update(INSERT_EXAMINEE_SQL) }
    }

    private fun eventIds(table: String): List<UUID> =
        jdbc.queryForList("SELECT md5('$table:' || history_id)::uuid FROM $table WHERE event_id IS NULL ORDER BY history_id", UUID::class.java)

    private companion object {
        val JSON = ObjectMapper()
        const val SCHEMA = "backfill_test"
        const val BUCKET = "backfill-test"
        const val POOL_SIZE = 2
        const val EVENT_ID = "event_id"
        const val EXAMINEE_ID = "examinee_id"
        const val EXAMINEE = "hg261026-a000011"
        const val TEST_SUB_HISTORY = "test_sub_history"
        const val EXAMINEE_HISTORY = "examinee_history"
        const val BIG_ROWS = 500
        /** 운영 기본값 16MB */
        const val DEFAULT_MAX_BATCH_BYTES = 16L * 1024 * 1024
        const val SMALL_MAX_BATCH_BYTES = 1L * 1024 * 1024
        /** COWAT rawData 크기 — 약 2.4MB */
        const val BIG_CHARS = 800_000
        const val SMALL_CHARS = 2_000

        const val TIME_ZONE_SQL = "SET TIME ZONE 'Asia/Seoul'"
        const val DROP_SCHEMA_SQL = "DROP SCHEMA IF EXISTS $SCHEMA CASCADE"
        /** 백필이 읽는 칸만 갖춘 예전 이력 테이블·회차 */
        const val CREATE_SCHEMA_SQL = """
            CREATE SCHEMA $SCHEMA;
            CREATE TABLE $SCHEMA.test (id varchar(30) PRIMARY KEY, examinee_id varchar(30));
            INSERT INTO $SCHEMA.test VALUES ('ht261026-a000011', '$EXAMINEE');
            CREATE TABLE $SCHEMA.test_sub_history (
                history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, id bigint, test_id varchar(30), raw_data jsonb,
                change_type varchar(20), changed_by varchar(50), change_reason text, device_info text, app_version varchar(30),
                changed_at timestamptz, created_at timestamptz, updated_at timestamptz, event_id uuid UNIQUE
            );
            CREATE TABLE $SCHEMA.examinee_history (
                history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, id varchar(30), name varchar(50),
                change_type varchar(20), changed_by varchar(50), change_reason text, device_info text, app_version varchar(30),
                changed_at timestamptz, created_at timestamptz, updated_at timestamptz, event_id uuid UNIQUE
            );
        """
        const val INSERT_TEST_SUB_SQL = """
            INSERT INTO test_sub_history (id, test_id, raw_data, change_type, changed_at)
            VALUES (?, 'ht261026-a000011', jsonb_build_object('answers', repeat('가', ?)), 'UPDATE', timestamptz '2026-10-26 10:00:00+09')
        """
        const val INSERT_EXAMINEE_SQL = """
            INSERT INTO examinee_history (id, name, change_type, created_at)
            VALUES ('$EXAMINEE', '홍길동', 'CREATE', timestamptz '2026-10-26 10:00:00+09')
        """
        const val MARK_RELAYED_SQL = "UPDATE test_sub_history SET event_id = gen_random_uuid() WHERE history_id = 2"
    }
}
