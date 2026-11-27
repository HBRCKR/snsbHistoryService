package com.hbrc.snsbhistory.relay

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.mockito.Mockito.mock
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.S3Exception
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val DB_URL_ENV = "HISTORY_TEST_DB_URL"

/**
 * 아웃박스 릴레이 — 로컬 PostgreSQL 에 붙어 돈다. [DB_URL_ENV] 가 있을 때만(예: jdbc:postgresql://localhost:5432/snsb_history_test?user=ksjhbrc).
 * 테스트 전용 스키마([SCHEMA])를 테스트마다 새로 만든다. S3 는 올린 파일을 기억하는 가짜 업로더.
 * 테스트 JVM 힙은 build.gradle.kts 에서 256MB 로 묶는다 — 큰 rawData 가 몰려도 릴레이가 멈추지 않는지 같은 조건에서 본다.
 */
@EnabledIfEnvironmentVariable(named = DB_URL_ENV, matches = ".+")
class OutboxRelayTest {
    /** 가짜 업로더에 올라간 파일 하나 — 종류, 줄의 event_id(순서대로)·history_type, 줄 크기 합계(줄바꿈 빼고) */
    private class Upload(val target: HistoryTarget, val eventIds: List<UUID>, val lineTypes: Set<String>, val bytes: Long)

    /** [failure] 는 올라온 줄의 event_id 로 실패를 흉내 낸다 */
    private class RecordingUploader(
        private val failure: (List<UUID>) -> Unit = {},
    ) : HistoryUploader(mock(S3Client::class.java), S3Properties(BUCKET)) {
        val uploads = CopyOnWriteArrayList<Upload>()
        val attempts = CopyOnWriteArrayList<List<UUID>>()

        override fun put(target: HistoryTarget, partition: String, fileName: String, gzipped: ByteArray, lines: Int) {
            val texts = GZIPInputStream(gzipped.inputStream()).bufferedReader().readLines()
            val eventIds = texts.map { UUID.fromString(JSON.readTree(it).get(EVENT_ID).asText()) }
            attempts.add(eventIds)
            failure(eventIds)
            uploads.add(Upload(target, eventIds, texts.map { JSON.readTree(it).get(HISTORY_TYPE).asText() }.toSet(), texts.sumOf { it.toByteArray().size.toLong() }))
        }

        fun delivered(): List<UUID> = uploads.flatMap { it.eventIds }
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
    fun `큰 행이 몰려도 읽는 크기를 상한까지로 잘라 순서대로 모두 올린다`() {
        // 재측정 때처럼 앞 500건 중 절반 가까이가 2.4MB 짜리(합계 약 600MB) — 256MB 힙에서 한 번에 읽으면 OOM
        insertRows(BATCH_SIZE) { if (it % 2 == 0) BIG_CHARS else SMALL_CHARS }
        val expected = outboxOrder()
        val uploader = RecordingUploader()

        drain(relay(uploader, DEFAULT_MAX_BATCH_BYTES))

        assertEquals(expected, uploader.delivered())
        uploader.uploads.filter { it.eventIds.size > 1 }.forEach {
            assertTrue(it.bytes <= DEFAULT_MAX_BATCH_BYTES, "여러 줄 묶음이 상한을 넘었다: ${it.bytes}")
        }
        assertEquals(0, count(OUTBOX))
        assertEquals(0, count(DEAD))
    }

    @Test
    fun `한 건이 상한보다 큰 행은 보내지 않고 dead 로 옮기고 나머지는 순서대로 올린다`() {
        // 순서는 넣은 순서(id) — 세 번째가 큰 행
        insertRows(4) { if (it == 3) BIG_CHARS else SMALL_CHARS }
        val expected = outboxOrder()
        val big = expected[2]
        val uploader = RecordingUploader()

        drain(relay(uploader, SMALL_MAX_BATCH_BYTES))

        assertEquals(expected - big, uploader.delivered())
        assertEquals(listOf(big), jdbc.queryForList(DEAD_EVENT_IDS_SQL, UUID::class.java))
        assertEquals(0, count(OUTBOX))
    }

    @Test
    fun `다른 인스턴스가 잠근 행은 건너뛰고 풀리면 올린다`() {
        insertRows(10) { SMALL_CHARS }
        val expected = outboxOrder()
        val uploader = RecordingUploader()
        val relay = relay(uploader, DEFAULT_MAX_BATCH_BYTES)

        dataSource.connection.use { other ->
            other.autoCommit = false
            other.createStatement().use { it.executeQuery(LOCK_HEAD_SQL).close() }
            drain(relay)
            assertEquals(expected.drop(LOCKED_HEAD), uploader.delivered())
            other.commit()
        }
        drain(relay)

        assertEquals(expected.drop(LOCKED_HEAD) + expected.take(LOCKED_HEAD), uploader.delivered())
        assertEquals(0, count(OUTBOX))
    }

    @Test
    fun `두 인스턴스가 동시에 비워도 한 건씩만 올리고 파일 안은 순서대로다`() {
        insertRows(BATCH_SIZE) { if (it % 5 == 0) BIG_CHARS else SMALL_CHARS }
        val expected = outboxOrder()
        val uploader = RecordingUploader()
        val relays = List(2) { relay(uploader, CONCURRENT_MAX_BATCH_BYTES, CONCURRENT_BATCH_SIZE) }

        val pool = Executors.newFixedThreadPool(relays.size)
        relays.map { pool.submit { drain(it) } }.forEach { it.get() }
        pool.shutdown()
        assertTrue(pool.awaitTermination(1, TimeUnit.MINUTES))
        // 한쪽이 먼저 끝난 사이 다른 쪽이 잠가 둔 행이 남았으면 마저 비운다
        drain(relays.first())

        assertEquals(expected.size, uploader.delivered().toSet().size)
        assertEquals(expected.sorted(), uploader.delivered().sorted())
        val position = expected.withIndex().associate { (index, id) -> id to index }
        uploader.uploads.forEach { upload ->
            val indexes = upload.eventIds.map { position.getValue(it) }
            assertEquals(indexes.sorted(), indexes)
        }
    }

    @Test
    fun `업로드가 실패하면 한 건씩 나눠 쓰지 않고 묶음 전체를 남겨 다음에 같은 묶음을 올린다`() {
        insertRows(5) { SMALL_CHARS }
        val expected = outboxOrder()
        var failing = true
        val uploader = RecordingUploader(failure = { if (failing) throw SdkClientException.create(UPLOAD_FAILED) })
        val relay = relay(uploader, DEFAULT_MAX_BATCH_BYTES)

        assertFailsWith<RuntimeException> { relay.relayBatch() }

        assertEquals(listOf(expected), uploader.attempts)
        assertEquals(5, count(OUTBOX))
        assertEquals(0, count(DEAD))

        failing = false
        drain(relay)

        assertEquals(expected, uploader.delivered())
        assertEquals(0, count(OUTBOX))
        assertEquals(0, count(DEAD))
    }

    @Test
    fun `형식이 깨진 행은 업로드가 실패해도 따로 커밋돼 dead 로 옮겨진다`() {
        insertRows(3) { SMALL_CHARS }
        jdbc.update(INSERT_MALFORMED_SQL, HistoryTarget.TEST_SUB.name)
        val uploader = RecordingUploader(failure = { throw SdkClientException.create(UPLOAD_FAILED) })

        assertFailsWith<RuntimeException> { relay(uploader, DEFAULT_MAX_BATCH_BYTES).relayBatch() }

        assertEquals(1, count(DEAD))
        assertEquals(3, count(OUTBOX))
    }

    @Test
    fun `종류가 섞여 있어도 한 묶음으로 id 순서대로 가져와 종류별 파일로 올린다`() {
        insertRows(3) { SMALL_CHARS }
        jdbc.update(INSERT_SQL, HistoryTarget.EXAMINEE.name, 0, SMALL_CHARS)
        insertRows(2) { SMALL_CHARS }
        val expected = outboxOrder()
        val uploader = RecordingUploader()

        relay(uploader, DEFAULT_MAX_BATCH_BYTES, flushIntervalMs = 0).poll()

        assertEquals(1, uploader.uploads.count { it.target == HistoryTarget.EXAMINEE })
        val position = expected.withIndex().associate { (index, id) -> id to index }
        uploader.uploads.forEach { upload ->
            assertEquals(setOf(upload.target.typeName), upload.lineTypes)
            val indexes = upload.eventIds.map { position.getValue(it) }
            assertEquals(indexes.sorted(), indexes)
        }
        assertEquals(expected.sorted(), uploader.delivered().sorted())
        assertEquals(0, count(OUTBOX))
    }

    @Test
    fun `모르는 종류는 가져가지 않고 아웃박스에 남긴다`() {
        insertRows(3) { SMALL_CHARS }
        jdbc.update(INSERT_SQL, UNKNOWN_TYPE, 0, SMALL_CHARS)
        val uploader = RecordingUploader()

        drain(relay(uploader, DEFAULT_MAX_BATCH_BYTES))

        assertEquals(3, uploader.delivered().size)
        assertEquals(1, count(OUTBOX))
        assertEquals(0, count(DEAD))
    }

    @Test
    fun `S3 장애면 어느 행도 dead 로 보내지 않고 아웃박스에 남긴다`() {
        insertRows(5) { SMALL_CHARS }
        val uploader = RecordingUploader(failure = { throw SdkClientException.create(UPLOAD_FAILED) })

        assertFailsWith<RuntimeException> { relay(uploader, DEFAULT_MAX_BATCH_BYTES).relayBatch() }

        assertEquals(5, count(OUTBOX))
        assertEquals(0, count(DEAD))
    }

    @Test
    fun `S3 설정 오류도 묶음째 장애로 남긴다`() {
        insertRows(5) { SMALL_CHARS }
        val uploader = RecordingUploader(failure = { throw S3Exception.builder().statusCode(FORBIDDEN).message(UPLOAD_FAILED).build() })

        assertFailsWith<RuntimeException> { relay(uploader, DEFAULT_MAX_BATCH_BYTES).relayBatch() }

        assertEquals(1, uploader.attempts.size)
        assertEquals(5, count(OUTBOX))
        assertEquals(0, count(DEAD))
    }

    @Test
    fun `형식이 깨진 행은 바로 dead 로 보내고 나머지는 올린다`() {
        insertRows(3) { SMALL_CHARS }
        jdbc.update(INSERT_MALFORMED_SQL, HistoryTarget.TEST_SUB.name)
        val good = jdbc.queryForList(GOOD_ORDER_SQL, UUID::class.java)
        val uploader = RecordingUploader()

        drain(relay(uploader, DEFAULT_MAX_BATCH_BYTES))

        assertEquals(good, uploader.delivered())
        assertEquals(1, count(DEAD))
        assertEquals(0, count(OUTBOX))
    }

    private fun relay(
        uploader: HistoryUploader,
        maxBatchBytes: Long,
        batchSize: Int = BATCH_SIZE,
        flushIntervalMs: Long = RelayProperties().flushIntervalMs,
    ) = OutboxRelay(
        jdbc,
        DataSourceTransactionManager(dataSource),
        uploader,
        RelayProperties(batchSize = batchSize, maxBatchBytes = maxBatchBytes, flushIntervalMs = flushIntervalMs),
        OutboxMetrics(SimpleMeterRegistry()),
    )

    private fun drain(relay: OutboxRelay) {
        while (relay.relayBatch() > 0) Unit
    }

    /**
     * 행 [count] 개를 DB 에서 만든다(본문을 JVM 에 올리지 않게). n 번째(1부터) 행의 rawData 는 [chars] (n) 글자 '가'(3바이트).
     * occurred_at 은 id 와 거꾸로, 세 건씩 같게 — 순서가 occurred_at 이 아니라 id 인지 본다
     */
    private fun insertRows(count: Int, chars: (Int) -> Int) {
        (1..count).forEach { n -> jdbc.update(INSERT_SQL, HistoryTarget.TEST_SUB.name, (count - n) / 3, chars(n)) }
    }

    private fun outboxOrder(): List<UUID> = jdbc.queryForList(ORDER_SQL, UUID::class.java)

    private fun count(table: String): Int = jdbc.queryForObject("SELECT count(*) FROM $table", Int::class.java) ?: 0

    private companion object {
        val JSON = ObjectMapper()
        const val SCHEMA = "relay_test"
        const val BUCKET = "relay-test"
        const val POOL_SIZE = 4
        const val EVENT_ID = "event_id"
        const val OUTBOX = "history_outbox"
        const val DEAD = "history_outbox_dead"
        const val UPLOAD_FAILED = "upload failed"
        const val FORBIDDEN = 403
        const val BATCH_SIZE = 500
        const val CONCURRENT_BATCH_SIZE = 50
        const val LOCKED_HEAD = 3
        /** 운영 기본값 16MB */
        const val DEFAULT_MAX_BATCH_BYTES = 16L * 1024 * 1024
        const val SMALL_MAX_BATCH_BYTES = 1L * 1024 * 1024
        /** 큰 행(약 2.4MB) 하나는 들어가지만 큰 행 둘은 못 들어가는 상한 — 동시에 비울 때도 크기에서 잘린다 */
        const val CONCURRENT_MAX_BATCH_BYTES = 3L * 1024 * 1024
        /** COWAT rawData 크기 — 약 2.4MB */
        const val BIG_CHARS = 800_000
        const val SMALL_CHARS = 2_000

        const val TIME_ZONE_SQL = "SET TIME ZONE 'Asia/Seoul'"
        const val DROP_SCHEMA_SQL = "DROP SCHEMA IF EXISTS $SCHEMA CASCADE"
        /** snsb3_api V12__tk671_history_outbox.sql 의 아웃박스 두 테이블 */
        const val CREATE_SCHEMA_SQL = """
            CREATE SCHEMA $SCHEMA;
            CREATE TABLE $SCHEMA.history_outbox (
                id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                event_id     uuid        NOT NULL,
                history_type varchar(20) NOT NULL,
                occurred_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
                payload      jsonb       NOT NULL,
                CONSTRAINT history_outbox_event_id_key UNIQUE (event_id)
            );
            CREATE INDEX history_outbox_relay_idx ON $SCHEMA.history_outbox (history_type, occurred_at, id);
            CREATE TABLE $SCHEMA.history_outbox_dead (
                id           bigint      PRIMARY KEY,
                event_id     uuid        NOT NULL,
                history_type varchar(20) NOT NULL,
                occurred_at  timestamptz NOT NULL,
                payload      jsonb       NOT NULL,
                last_error   text,
                dead_at      timestamptz NOT NULL DEFAULT now(),
                CONSTRAINT history_outbox_dead_event_id_key UNIQUE (event_id)
            );
        """
        const val INSERT_SQL = """
            INSERT INTO history_outbox (event_id, history_type, occurred_at, payload)
            VALUES (gen_random_uuid(), ?, timestamptz '2026-10-26 10:00:00+09' + ? * interval '1 millisecond',
                    jsonb_build_object('change_type', 'UPDATE', 'payload', jsonb_build_object('rawData', repeat('가', ?))))
        """
        const val INSERT_MALFORMED_SQL = """
            INSERT INTO history_outbox (event_id, history_type, occurred_at, payload)
            VALUES (gen_random_uuid(), ?, timestamptz '2026-10-26 09:59:59+09', '{"payload": "not an object"}')
        """
        const val ORDER_SQL = "SELECT event_id FROM history_outbox ORDER BY id"
        const val GOOD_ORDER_SQL = "SELECT event_id FROM history_outbox WHERE payload -> 'change_type' IS NOT NULL ORDER BY id"
        const val LOCK_HEAD_SQL = "SELECT id FROM history_outbox ORDER BY id LIMIT $LOCKED_HEAD FOR UPDATE"
        const val DEAD_EVENT_IDS_SQL = "SELECT event_id FROM history_outbox_dead"
        const val HISTORY_TYPE = "history_type"
        /** 이 서비스가 아직 모르는 종류(아웃박스 history_type 은 varchar(20)) */
        const val UNKNOWN_TYPE = "FUTURE_TYPE"
    }
}
