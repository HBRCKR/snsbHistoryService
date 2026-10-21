package com.hbrc.snsbhistory

import com.hbrc.snsbhistory.relay.S3Properties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.URI

/** snsb3_api 가 남긴 이력 아웃박스(history_outbox)를 모아 S3 에 올리는 서비스 — PMWEB-TK-671 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
class SnsbHistoryApplication {
    /** 자격 증명은 기본 체인(EC2 인스턴스 역할, 로컬은 AWS_ACCESS_KEY_ID·AWS_SECRET_ACCESS_KEY) */
    @Bean
    fun s3Client(properties: S3Properties): S3Client = S3Client.builder()
        .region(Region.of(properties.region))
        .apply {
            if (properties.endpoint.isNotBlank()) {
                endpointOverride(URI.create(properties.endpoint))
                forcePathStyle(true)
            }
        }
        .build()
}

fun main(args: Array<String>) {
    runApplication<SnsbHistoryApplication>(*args)
}
