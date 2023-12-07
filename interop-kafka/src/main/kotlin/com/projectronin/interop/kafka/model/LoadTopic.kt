package com.projectronin.interop.kafka.model

import com.projectronin.event.interop.internal.v1.ResourceType

/**
 * A LoadTopic is a [KafkaTopic] associated to a load workflow within Interops.
 */
data class LoadTopic(
    override val systemName: String,
    override val topicName: String,
    override val dataSchema: String,
    val resourceType: ResourceType,
) : KafkaTopic {
    override val useLatestOffset = false
}
