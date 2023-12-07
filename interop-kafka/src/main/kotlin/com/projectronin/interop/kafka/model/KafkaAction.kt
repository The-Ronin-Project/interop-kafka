package com.projectronin.interop.kafka.model

/**
 * Enumeration of the supported actions.
 */
enum class KafkaAction(val type: String) {
    PUBLISH("publish"),
    RETRIEVE("retrieve"),
    LOAD("load"),
    CREATE("create"),
    UPDATE("update"),
    REQUEST("request"),
}
