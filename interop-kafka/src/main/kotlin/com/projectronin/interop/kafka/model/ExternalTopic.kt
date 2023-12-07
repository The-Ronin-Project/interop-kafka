package com.projectronin.interop.kafka.model

/**
 * A [ExternalTopic] is the topic that services external to interops
 */
data class ExternalTopic(
    override val systemName: String,
    override val topicName: String,
    override val dataSchema: String,
) : KafkaTopic {
    override val useLatestOffset = false
}
