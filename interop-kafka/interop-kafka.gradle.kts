plugins {
    alias(libs.plugins.interop.gradle.integration)
    alias(libs.plugins.interop.gradle.spring)
}

dependencies {
    implementation(libs.interop.fhir)
    implementation(libs.interop.common)
    implementation(libs.interop.commonJackson)
    implementation(libs.ronin.kafka)
    implementation(libs.bundles.interop.kafka.events)
    implementation(libs.dd.trace.api)

    implementation(platform(libs.spring.boot.parent))
    implementation("org.springframework.boot:spring-boot")

    testImplementation(libs.mockk)
    testImplementation("org.springframework:spring-test")
    testImplementation("org.springframework.boot:spring-boot-starter")

    testImplementation(platform(libs.testcontainers.bom))

    itImplementation(platform(libs.testcontainers.bom))
    itImplementation("org.testcontainers:junit-jupiter")
    itImplementation(libs.interop.common)
    itImplementation(libs.interop.commonJackson)
    itImplementation(libs.ronin.kafka)
    itImplementation(libs.interop.fhir)
    itImplementation(libs.bundles.interop.kafka.events)
    itImplementation(libs.kotlinx.coroutines.core)
    itImplementation("io.github.microutils:kotlin-logging:3.0.5")
}
