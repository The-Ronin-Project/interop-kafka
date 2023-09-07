plugins {
    alias(libs.plugins.interop.gradle.junit)
    alias(libs.plugins.interop.gradle.spring)
    alias(libs.plugins.interop.gradle.integration)
    alias(libs.plugins.interop.gradle.publish)
    alias(libs.plugins.interop.version.catalog)
}

subprojects {
    apply(plugin = "com.projectronin.interop.gradle.publish")
}
