package com.hbrc.snsbhistory.relay

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * @param batchSize 한 번에 잠가서 올리는 outbox 행 수(파일 하나 — 시간 칸이 바뀌면 칸마다 하나). 이만큼 쌓이면 바로 올린다
 * @param flushIntervalMs 가장 오래된 이력이 이만큼 기다렸으면 덜 차도 올린다
 * @param pollIntervalMs 올릴 때가 됐는지 보는 주기
 * @param maxBatchBytes 묶음 하나(파일)에 담는 줄의 합계 크기 상한 — 메모리 보호. 줄 크기를 DB 에서 먼저 재서 이만큼만 읽어 온다.
 *   한 건이 이보다 크면 보내지 않고 dead 로(실제 최대 rawData 는 수 MB — 넉넉히 위)
 * @param backoffInitialMs 업로드 실패(S3·DB 장애) 때 첫 재시도 간격 — 같은 묶음을 다시 올린다. 실패할 때마다 두 배, [backoffMaxMs] 까지
 * @param metricsIntervalMs 적체·dead 지표를 다시 세는 주기
 */
@ConfigurationProperties("relay")
data class RelayProperties(
    val batchSize: Int = 500,
    val flushIntervalMs: Long = 60_000,
    val pollIntervalMs: Long = 10_000,
    val maxBatchBytes: Long = 16L * 1024 * 1024,
    val backoffInitialMs: Long = 1_000,
    val backoffMaxMs: Long = 60_000,
    val metricsIntervalMs: Long = 15_000,
)

/**
 * @param bucket 이력 전용 버킷
 * @param prefix 버킷 안 경로 앞부분(비우면 버킷 맨 위가 history_type)
 * @param endpoint 비우면 AWS. 로컬 MinIO 면 http://localhost:9000 처럼 넣는다(경로 방식 주소로 바뀐다)
 * @param callTimeoutMs 업로드 한 번(SDK 재시도 포함)의 전체 시간 상한 — 넘으면 실패로 보고 묶음을 롤백한다
 * @param attemptTimeoutMs 그중 시도 한 번의 시간 상한(16MB 묶음 기준으로 넉넉히)
 */
@ConfigurationProperties("s3")
data class S3Properties(
    val bucket: String,
    val prefix: String = "",
    val region: String = "ap-northeast-2",
    val endpoint: String = "",
    val callTimeoutMs: Long = 30_000,
    val attemptTimeoutMs: Long = 10_000,
)

/**
 * 기존 이력 테이블 → S3 한 번 올리기(docs/history-outbox-s3-plan.md 8절 1단계).
 * @param enabled 켜면 앱이 뜰 때 백필만 하고 끝낸다(폴링하지 않는다)
 * @param batchSize 한 번에 읽는 이력 행 수
 * @param maxBatchBytes 한 번에 읽어 올리는 줄의 합계 크기 상한 — 메모리 보호. 줄 크기를 DB 에서 먼저 재서 이만큼만 읽어 온다.
 *   한 건이 이보다 크면 그 한 건만 파일 하나로
 */
@ConfigurationProperties("backfill")
data class BackfillProperties(
    val enabled: Boolean = false,
    val batchSize: Int = 5_000,
    val maxBatchBytes: Long = 16L * 1024 * 1024,
)
