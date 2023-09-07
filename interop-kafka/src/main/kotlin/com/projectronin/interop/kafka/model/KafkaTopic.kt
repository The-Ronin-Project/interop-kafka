package com.projectronin.interop.kafka.model

/**
 * A KafkaTopic is a representation of a topic as defined by the Ronin standards.
 */
interface KafkaTopic {
    val useLatestOffset: Boolean
    val systemName: String
    val topicName: String
    val dataSchema: String
}
