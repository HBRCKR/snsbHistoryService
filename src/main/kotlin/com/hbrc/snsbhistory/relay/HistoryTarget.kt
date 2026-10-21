package com.hbrc.snsbhistory.relay

import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * 이력 종류. 이름은 snsb3_api 의 HistoryType 과 같아야 한다(history_outbox.history_type 값).
 * @param typeName S3 경로 첫 칸·줄의 history_type
 * @param historyTable 예전 이력 테이블 — 전환 기간 이중 쓰기·백필 대상. 직원 이력은 예전 테이블이 없어 S3 에만 쓴다(빈 값)
 */
enum class HistoryTarget(val typeName: String, val historyTable: String) {
    TEST_SUB("test_sub", "test_sub_history"),
    EXAMINEE("examinee", "examinee_history"),
    STAFF("staff", ""),
    ;

    /** 예전 이력 테이블이 있나 — 이중 쓰기·백필은 있는 종류만 */
    val hasHistoryTable: Boolean get() = historyTable.isNotEmpty()
}

/** S3 한 줄 형식 버전(docs/history-outbox-s3-plan.md 4절) */
const val SCHEMA_VERSION = 1

/** 이력 시각·S3 경로(dt/hour)의 기준 — 한국 시간. DB 연결 시간대도 이것으로 맞춘다(application.yml connection-init-sql) */
val HISTORY_ZONE: ZoneId = ZoneId.of("Asia/Seoul")

private val DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd")
private val HOUR = DateTimeFormatter.ofPattern("HH")

/** 파일이 들어갈 시간 칸 — 한국 시간 날짜·시 */
fun hourPartition(at: OffsetDateTime): String {
    val local = at.atZoneSameInstant(HISTORY_ZONE)
    return "dt=${local.format(DATE)}/hour=${local.format(HOUR)}"
}

/** {prefix/}{history_type}/dt=YYYY-MM-DD/hour=HH/{fileName}.jsonl.gz */
fun objectKey(prefix: String, target: HistoryTarget, partition: String, fileName: String): String {
    val head = if (prefix.isBlank()) "" else "${prefix.trimEnd('/')}/"
    return "$head${target.typeName}/$partition/$fileName.jsonl.gz"
}
