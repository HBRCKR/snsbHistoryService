package com.hbrc.snsbhistory.relay

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

/**
 * 운영 지표(docs/history-outbox-s3-plan.md 7절) — /actuator/metrics 로 본다. CloudWatch 알림은 이 값으로 건다.
 * - history.outbox.backlog{type}: 아직 안 올라간 outbox 행 수
 * - history.outbox.oldest.age.seconds{type}: 가장 오래된 outbox 행이 기다린 시간
 * - history.outbox.dead: dead 행 수
 * - history.s3.put.failures: S3 업로드 실패 횟수(장애로 본 것)
 * - history.s3.config.errors: S3 설정 오류(권한·버킷 없음) — 사람이 고쳐야 한다
 * - history.dead.moved{type}: dead 로 옮긴 이력 수
 * - history.relayed{type}: S3 에 올린 이력 수
 * 적체·dead 는 [RelayProperties.metricsIntervalMs] 마다 다시 센다(지표를 읽을 때마다 DB 를 두드리지 않게).
 */
@Component
class OutboxMetrics(private val jdbc: JdbcTemplate, registry: MeterRegistry) {
    private val backlog = HistoryTarget.entries.associateWith { AtomicLong() }
    private val oldestAgeSeconds = HistoryTarget.entries.associateWith { AtomicLong() }
    private val dead = AtomicLong()
    private val s3Failures: Counter = Counter.builder(S3_FAILURES).register(registry)
    private val s3ConfigErrors: Counter = Counter.builder(S3_CONFIG_ERRORS).register(registry)
    private val deadMoved = HistoryTarget.entries.associateWith {
        Counter.builder(DEAD_MOVED).tag(TYPE_TAG, it.typeName).register(registry)
    }
    private val relayed = HistoryTarget.entries.associateWith {
        Counter.builder(RELAYED).tag(TYPE_TAG, it.typeName).register(registry)
    }

    init {
        HistoryTarget.entries.forEach { target ->
            Gauge.builder(BACKLOG, backlog.getValue(target)) { it.toDouble() }.tag(TYPE_TAG, target.typeName).register(registry)
            Gauge.builder(OLDEST_AGE, oldestAgeSeconds.getValue(target)) { it.toDouble() }.tag(TYPE_TAG, target.typeName).register(registry)
        }
        Gauge.builder(DEAD, dead) { it.toDouble() }.register(registry)
    }

    @Scheduled(fixedDelayString = "\${relay.metrics-interval-ms:15000}")
    fun refresh() {
        HistoryTarget.entries.forEach { backlog.getValue(it).set(0); oldestAgeSeconds.getValue(it).set(0) }
        jdbc.query(BACKLOG_SQL) { rs ->
            // 모르는 종류(앱이 먼저 새 종류를 넣은 경우)는 지표에서 뺀다
            HistoryTarget.entries.firstOrNull { it.name == rs.getString(1) }?.let { target ->
                backlog.getValue(target).set(rs.getLong(2))
                oldestAgeSeconds.getValue(target).set(rs.getLong(3))
            }
        }
        dead.set(jdbc.queryForObject(DEAD_SQL, Long::class.java) ?: 0)
    }

    fun s3Failed() = s3Failures.increment()

    fun s3ConfigError() = s3ConfigErrors.increment()

    fun movedToDead(target: HistoryTarget) = deadMoved.getValue(target).increment()

    fun relayed(target: HistoryTarget, count: Int) = relayed.getValue(target).increment(count.toDouble())

    private companion object {
        const val BACKLOG = "history.outbox.backlog"
        const val OLDEST_AGE = "history.outbox.oldest.age.seconds"
        const val DEAD = "history.outbox.dead"
        const val S3_FAILURES = "history.s3.put.failures"
        const val S3_CONFIG_ERRORS = "history.s3.config.errors"
        const val DEAD_MOVED = "history.dead.moved"
        const val RELAYED = "history.relayed"
        const val TYPE_TAG = "type"
        const val BACKLOG_SQL = """
            SELECT history_type, count(*), coalesce(extract(epoch FROM clock_timestamp() - min(occurred_at)), 0)::bigint
              FROM history_outbox GROUP BY history_type
        """
        const val DEAD_SQL = "SELECT count(*) FROM history_outbox_dead"
    }
}
