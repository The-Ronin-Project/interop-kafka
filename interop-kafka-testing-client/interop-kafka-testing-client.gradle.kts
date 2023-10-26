dependencies {
    implementation(project(":interop-kafka"))
    implementation(libs.ronin.kafka)
    implementation(libs.bundles.interop.kafka.events)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2") // replace 1.5.2 with the latest version
    implementation(libs.interop.fhir)
}

sonar {
    isSkipProject = true
}
