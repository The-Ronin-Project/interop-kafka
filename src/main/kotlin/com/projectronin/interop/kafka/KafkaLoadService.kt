package com.projectronin.interop.kafka

import com.projectronin.event.interop.internal.v1.InteropResourceLoadV1
import com.projectronin.event.interop.internal.v1.Metadata
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.event.interop.internal.v1.eventName
import com.projectronin.interop.kafka.client.KafkaClient
import com.projectronin.interop.kafka.model.DataTrigger
import com.projectronin.interop.kafka.model.Failure
import com.projectronin.interop.kafka.model.KafkaAction
import com.projectronin.interop.kafka.model.KafkaEvent
import com.projectronin.interop.kafka.model.LoadTopic
import com.projectronin.interop.kafka.model.PushResponse
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.time.Duration

/**
 * Service responsible for creating load events to Kafka
 */
@Service
class KafkaLoadService(private val kafkaClient: KafkaClient, topics: List<LoadTopic>) {
    private val logger = KotlinLogging.logger { }

    private val loadTopicsByResourceType = topics.groupBy { it.resourceType }

    /**
     * Triggers a Load event for the [resourceFHIRIds] to the appropriate Kafka topics for [tenantId].
     */
    fun pushLoadEvent(
        tenantId: String,
        trigger: DataTrigger,
        resourceFHIRIds: List<String>,
        resourceType: ResourceType,
        metadata: Metadata,
        flowOptions: InteropResourceLoadV1.FlowOptions? = null
    ): PushResponse<String> {
        val loadTopic = getTopic(resourceType)
        if (loadTopic == null) {
            logger.error { "No matching LoadTopics associated to resource type $resourceType" }
            return PushResponse(
                failures = resourceFHIRIds.map {
                    Failure(
                        it,
                        IllegalStateException("Zero or multiple LoadTopics associated to resource type $resourceType")
                    )
                }
            )
        } else {
            val events = resourceFHIRIds.map {
                KafkaEvent(
                    domain = loadTopic.systemName,
                    resource = resourceType.eventName(),
                    action = KafkaAction.LOAD,
                    resourceId = it,
                    data = InteropResourceLoadV1(
                        tenantId = tenantId,
                        resourceFHIRId = it,
                        resourceType = resourceType,
                        dataTrigger = when (trigger) {
                            DataTrigger.AD_HOC -> InteropResourceLoadV1.DataTrigger.adhoc
                            DataTrigger.NIGHTLY -> InteropResourceLoadV1.DataTrigger.nightly
                        },
                        metadata = metadata,
                        flowOptions = flowOptions
                    )
                )
            }

            return runCatching { kafkaClient.publishEvents(loadTopic, events) }.fold(
                onSuccess = { response ->
                    PushResponse(
                        successful = response.successful.map { it.data.resourceFHIRId },
                        failures = response.failures.map { Failure(it.data.data.resourceFHIRId, it.error) }
                    )
                },
                onFailure = { exception ->
                    logger.error(exception) { "Exception while attempting to publish events to $loadTopic" }
                    PushResponse(
                        failures = events.map { Failure(it.data.resourceFHIRId, exception) }
                    )
                }
            )
        }
    }

    /**
     * Grabs Load-style events from Kafka.
     * If [justClear] is set, will simply drain the current events (useful for testing).
     */
    fun retrieveLoadEvents(
        resourceType: ResourceType,
        groupId: String? = null,
        justClear: Boolean = false
    ): List<InteropResourceLoadV1> {
        val topic = getTopic(resourceType) ?: return emptyList()
        val typeMap = mapOf("ronin.interop-mirth.${resourceType.eventName()}.load" to InteropResourceLoadV1::class)
        if (justClear) {
            // shorter wait time because you are assuming events are there or not, no waiting
            kafkaClient.retrieveEvents(topic, typeMap, groupId, Duration.ofMillis(500))
            return emptyList()
        }
        val events = kafkaClient.retrieveEvents(topic, typeMap, groupId)
        return events.map {
            it.data as InteropResourceLoadV1
        }
    }

    fun deleteAllLoadTopics() {
        kafkaClient.deleteTopics(loadTopicsByResourceType.values.flatten().distinct())
    }

    fun getTopic(resourceType: ResourceType): LoadTopic? {
        return loadTopicsByResourceType[resourceType]?.singleOrNull()
    }
}
