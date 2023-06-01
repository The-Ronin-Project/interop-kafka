package com.projectronin.interop.kafka

import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.event.interop.resource.request.v1.InteropResourceRequestV1
import com.projectronin.interop.kafka.client.KafkaClient
import com.projectronin.interop.kafka.model.Failure
import com.projectronin.interop.kafka.model.KafkaAction
import com.projectronin.interop.kafka.model.KafkaEvent
import com.projectronin.interop.kafka.model.PushResponse
import com.projectronin.interop.kafka.model.RequestTopic
import mu.KotlinLogging
import org.springframework.stereotype.Service

@Service
class KafkaRequestService(private val kafkaClient: KafkaClient, private val topic: RequestTopic) {
    private val logger = KotlinLogging.logger { }

    fun pushRequestEvent(
        tenantId: String,
        resourceFHIRIds: List<String>,
        resourceType: ResourceType,
        requestingService: String
    ): PushResponse<String> {
        val events = resourceFHIRIds.map {
            KafkaEvent(
                domain = topic.systemName,
                resource = "resource",
                action = KafkaAction.REQUEST,
                resourceId = it,
                data = InteropResourceRequestV1(
                    tenantId = tenantId,
                    resourceFHIRId = it,
                    resourceType = resourceType.name,
                    requestingService = requestingService
                )
            )
        }

        return runCatching { kafkaClient.publishEvents(topic, events) }.fold(
            onSuccess = { response ->
                PushResponse(
                    successful = response.successful.map { it.data.resourceFHIRId },
                    failures = response.failures.map { Failure(it.data.data.resourceFHIRId, it.error) }
                )
            },
            onFailure = { exception ->
                logger.error(exception) { "Exception while attempting to publish events to $topic" }
                PushResponse(
                    failures = events.map { Failure(it.data.resourceFHIRId, exception) }
                )
            }
        )
    }

    fun retrieveRequestEvents(
        groupId: String? = null
    ): List<InteropResourceRequestV1> {
        val typeMap = mapOf("ronin.interop-mirth.resource.request" to InteropResourceRequestV1::class)
        val events = kafkaClient.retrieveEvents(topic, typeMap, groupId)
        return events.map {
            it.data as InteropResourceRequestV1
        }
    }
}
