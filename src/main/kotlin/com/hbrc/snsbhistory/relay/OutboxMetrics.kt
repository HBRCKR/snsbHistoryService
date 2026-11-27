package com.hbrc.snsbhistory.relay

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

/**
 * relay 가 한 일(사건) 지표 — /actuator/prometheus. 인스턴스마다 따로 세니 여러 대면 sum(rate(...)) 로 합쳐 본다.
 * - history.relayed{type}: S3 에 올린 이력 수
 * - history.dead.moved{type}: dead 로 옮긴 이력 수
 * - history.s3.put.failures: S3 업로드 실패 횟수(장애로 본 것)
 * - history.s3.config.errors: S3 설정 오류(권한·버킷 없음) — 사람이 고쳐야 한다
 *
 * 아웃박스 상태(적체·가장 오래된 이력 나이·dead 수)는 여기서 세지 않는다 — relay 가 꺼지면 그 지표도 같이 사라지고,
 * 여러 대면 같은 값을 대수만큼 낸다. 상태는 relay 밖의 outbox-monitor(Lambda)가 DB 를 직접 재서 CloudWatch 로 올린다.
 */
@Component
class OutboxMetrics(registry: MeterRegistry) {
    private val s3Failures: Counter = Counter.builder(S3_FAILURES).register(registry)
    private val s3ConfigErrors: Counter = Counter.builder(S3_CONFIG_ERRORS).register(registry)
    private val deadMoved = HistoryTarget.entries.associateWith {
        Counter.builder(DEAD_MOVED).tag(TYPE_TAG, it.typeName).register(registry)
    }
    private val relayed = HistoryTarget.entries.associateWith {
        Counter.builder(RELAYED).tag(TYPE_TAG, it.typeName).register(registry)
    }

    fun s3Failed() = s3Failures.increment()

    fun s3ConfigError() = s3ConfigErrors.increment()

    fun movedToDead(target: HistoryTarget, count: Int) = deadMoved.getValue(target).increment(count.toDouble())

    fun relayed(target: HistoryTarget, count: Int) = relayed.getValue(target).increment(count.toDouble())

    private companion object {
        const val S3_FAILURES = "history.s3.put.failures"
        const val S3_CONFIG_ERRORS = "history.s3.config.errors"
        const val DEAD_MOVED = "history.dead.moved"
        const val RELAYED = "history.relayed"
        const val TYPE_TAG = "type"
    }
}
