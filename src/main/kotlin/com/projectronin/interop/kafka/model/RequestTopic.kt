package com.projectronin.interop.kafka.model

/**
 * A [RequestTopic] is the topic that services external to interops, publish to request a specific resource
 */
data class RequestTopic(
    override val systemName: String,
    override val topicName: String,
    override val dataSchema: String
) : KafkaTopic
