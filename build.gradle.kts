plugins {
    kotlin("jvm") version "1.9.25"
    kotlin("plugin.spring") version "1.9.25"
    id("org.springframework.boot") version "3.4.4"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.hbrc"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // /actuator/prometheus — 적체·oldest age 를 Prometheus(ADOT) 로 긁어 알림을 건다
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation(platform("software.amazon.awssdk:bom:2.29.52"))
    implementation("software.amazon.awssdk:s3")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    runtimeOnly("org.postgresql:postgresql")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
    // OOM 재현 조건(-Xmx256m) — 큰 rawData 가 몰려도 릴레이가 멈추지 않는지 같은 힙에서 본다(OutboxRelayTest)
    maxHeapSize = "256m"
    // OutboxRelayTest 는 이 값(로컬 PostgreSQL)이 있을 때만 돈다 — 바뀌면 다시 돌게 입력으로 둔다
    inputs.property("historyTestDbUrl", providers.environmentVariable("HISTORY_TEST_DB_URL").orElse(""))
}
