package com.projectronin.interop.kafka

import com.projectronin.event.interop.internal.v1.InteropResourcePublishV1
import com.projectronin.event.interop.internal.v1.Metadata
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.event.interop.internal.v1.eventName
import com.projectronin.interop.fhir.r4.resource.Resource
import com.projectronin.interop.kafka.client.KafkaClient
import com.projectronin.interop.kafka.model.DataTrigger
import com.projectronin.interop.kafka.model.Failure
import com.projectronin.interop.kafka.model.KafkaAction
import com.projectronin.interop.kafka.model.KafkaEvent
import com.projectronin.interop.kafka.model.PublishTopic
import com.projectronin.interop.kafka.model.PushResponse
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.time.Duration

/**
 * Service responsible for publishing various events to Kafka.
 */
@Service
class KafkaPublishService(private val kafkaClient: KafkaClient, topics: List<PublishTopic>) {
    private val logger = KotlinLogging.logger { }

    private val publishTopicsByResourceType = topics.groupBy { Pair(it.resourceType, it.dataTrigger) }

    /**
     * Publishes the [resources] to the appropriate Kafka topics for [tenantId].
     */
    fun publishResources(
        tenantId: String,
        trigger: DataTrigger,
        resources: List<Resource<*>>,
        metadata: Metadata
    ): PushResponse<Resource<*>> {
        val resourcesByType = resources.groupBy { ResourceType.valueOf(it.resourceType) }
        val results = resourcesByType.map { (type, resources) ->
            val publishTopic = getTopic(type, trigger)
            if (publishTopic == null) {
                logger.error { "No matching PublishTopics associated to resource type $type and trigger $trigger" }
                PushResponse(
                    failures = resources.map {
                        Failure(
                            it,
                            IllegalStateException("Zero or multiple PublishTopics associated to resource type $type")
                        )
                    }
                )
            } else {
                val events = resources.associateBy {
                    KafkaEvent(
                        domain = publishTopic.systemName,
                        resource = type.eventName(),
                        action = KafkaAction.PUBLISH,
                        resourceId = it.id!!.value!!,
                        data = publishTopic.converter(tenantId, it, metadata)
                    )
                }

                runCatching { kafkaClient.publishEvents(publishTopic, events.keys.toList()) }.fold(
                    onSuccess = { response ->
                        PushResponse(
                            successful = response.successful.map { events[it]!! },
                            failures = response.failures.map { Failure(events[it.data]!!, it.error) }
                        )
                    },
                    onFailure = { exception ->
                        logger.error(exception) { "Exception while attempting to publish events to $publishTopic" }
                        PushResponse(
                            failures = events.map { Failure(it.value, exception) }
                        )
                    }
                )
            }
        }
        return PushResponse(
            successful = results.flatMap { it.successful },
            failures = results.flatMap { it.failures }
        )
    }

    /**
     * Grabs Publish-style events from Kafka.
     * If [justClear] is set, will simply drain the current events (useful for testing).
     */
    fun retrievePublishEvents(
        resourceType: ResourceType,
        dataTrigger: DataTrigger,
        groupId: String? = null,
        justClear: Boolean = false
    ): List<InteropResourcePublishV1> {
        val topic = getTopic(resourceType, dataTrigger)
            ?: return emptyList()
        val typeMap =
            mapOf("ronin.interop-mirth.${resourceType.eventName()}.publish" to InteropResourcePublishV1::class)
        if (justClear) {
            kafkaClient.retrieveEvents(
                topic = topic,
                typeMap = typeMap,
                groupId = groupId,
                // shorter wait time because you are assuming events are there or not, no waiting
                duration = Duration.ofMillis(500)
            )
            return emptyList()
        }
        val events = kafkaClient.retrieveEvents(
            topic = topic,
            typeMap = typeMap,
            groupId = groupId
        )
        return events.map {
            it.data as InteropResourcePublishV1
        }
    }

    private fun getTopic(resourceType: ResourceType, dataTrigger: DataTrigger): PublishTopic? {
        return publishTopicsByResourceType[Pair(resourceType, dataTrigger)]?.singleOrNull()
    }
}
