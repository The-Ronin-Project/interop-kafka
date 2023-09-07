package com.projectronin.interop.kafka.model

import com.projectronin.event.interop.internal.v1.Metadata
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.interop.fhir.r4.resource.Resource

/**
 * A PublishTopic is a [KafkaTopic] associated to a publish workflow within Interops.
 */
data class PublishTopic(
    override val systemName: String,
    override val topicName: String,
    override val dataSchema: String,
    val resourceType: ResourceType,
    val dataTrigger: DataTrigger,
    val converter: (String, Resource<*>, Metadata) -> Any
) : KafkaTopic {
    override val useLatestOffset = true
}
