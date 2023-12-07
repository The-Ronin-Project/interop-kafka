package com.projectronin.interop.kafka.spring

import com.projectronin.event.interop.internal.v1.InteropResourcePublishV1
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.event.interop.internal.v1.eventName
import com.projectronin.interop.common.jackson.JacksonManager.Companion.objectMapper
import com.projectronin.interop.kafka.model.DataTrigger
import com.projectronin.interop.kafka.model.PublishTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class PublishSpringConfig(private val kafkaSpringConfig: KafkaConfig) {
    @Bean
    fun publishTopics(): List<PublishTopic> {
        val supportedResources =
            listOf(
                ResourceType.Patient,
                ResourceType.Binary,
                ResourceType.Practitioner,
                ResourceType.Appointment,
                ResourceType.CarePlan,
                ResourceType.CareTeam,
                ResourceType.Communication,
                ResourceType.Condition,
                ResourceType.DocumentReference,
                ResourceType.Encounter,
                ResourceType.Location,
                ResourceType.Medication,
                ResourceType.MedicationAdministration,
                ResourceType.MedicationRequest,
                ResourceType.MedicationStatement,
                ResourceType.Observation,
                ResourceType.Organization,
                ResourceType.PractitionerRole,
                ResourceType.ServiceRequest,
                ResourceType.Procedure,
                ResourceType.RequestGroup,
                ResourceType.DiagnosticReport,
            )
        return supportedResources.map {
            generateTopics(it)
        }.flatten()
    }

    @Suppress("ktlint:standard:max-line-length")
    fun generateTopics(resourceType: ResourceType): List<PublishTopic> {
        val system = kafkaSpringConfig.retrieve.serviceId
        val topicParameters =
            listOf(
                kafkaSpringConfig.cloud.vendor,
                kafkaSpringConfig.cloud.region,
                "interop-mirth",
                "${resourceType.eventName()}-publish-nightly",
                "v1",
            )
        val nightlyTopic =
            PublishTopic(
                systemName = system,
                topicName = topicParameters.joinToString("."),
                dataSchema = "https://github.com/projectronin/contract-event-interop-resource-publish/blob/main/v1/resource-publish-v1.schema.json",
                resourceType = resourceType,
                dataTrigger = DataTrigger.NIGHTLY,
                converter = { tenant, resourceWrapper, metadata ->
                    val resource = resourceWrapper.resource
                    InteropResourcePublishV1(
                        tenantId = tenant,
                        resourceJson = objectMapper.writeValueAsString(resource),
                        resourceType = ResourceType.valueOf(resource.resourceType),
                        dataTrigger = InteropResourcePublishV1.DataTrigger.nightly,
                        metadata = metadata,
                        embeddedResources =
                            resourceWrapper.embeddedResources.map { embedded ->
                                InteropResourcePublishV1.EmbeddedResource(
                                    resourceType = ResourceType.valueOf(embedded.resourceType),
                                    resourceJson = objectMapper.writeValueAsString(embedded),
                                )
                            },
                    )
                },
            )

        val adHocTopic =
            PublishTopic(
                systemName = system,
                topicName = "oci.us-phoenix-1.interop-mirth.${resourceType.eventName()}-publish-adhoc.v1",
                dataSchema = "https://github.com/projectronin/contract-event-interop-resource-publish/blob/main/v1/resource-publish-v1.schema.json",
                resourceType = resourceType,
                dataTrigger = DataTrigger.AD_HOC,
                converter = { tenant, resourceWrapper, metadata ->
                    val resource = resourceWrapper.resource
                    InteropResourcePublishV1(
                        tenantId = tenant,
                        resourceJson = objectMapper.writeValueAsString(resource),
                        resourceType = ResourceType.valueOf(resource.resourceType),
                        dataTrigger = InteropResourcePublishV1.DataTrigger.adhoc,
                        metadata = metadata,
                        embeddedResources =
                            resourceWrapper.embeddedResources.map { embedded ->
                                InteropResourcePublishV1.EmbeddedResource(
                                    resourceType = ResourceType.valueOf(embedded.resourceType),
                                    resourceJson = objectMapper.writeValueAsString(embedded),
                                )
                            },
                    )
                },
            )

        val backfillTopic =
            PublishTopic(
                systemName = system,
                topicName = "oci.us-phoenix-1.interop-mirth.${resourceType.eventName()}-publish-adhoc.v1",
                dataSchema = "https://github.com/projectronin/contract-event-interop-resource-publish/blob/main/v1/resource-publish-v1.schema.json",
                resourceType = resourceType,
                dataTrigger = DataTrigger.BACKFILL,
                converter = { tenant, resourceWrapper, metadata ->
                    val resource = resourceWrapper.resource
                    InteropResourcePublishV1(
                        tenantId = tenant,
                        resourceJson = objectMapper.writeValueAsString(resource),
                        resourceType = ResourceType.valueOf(resource.resourceType),
                        dataTrigger = InteropResourcePublishV1.DataTrigger.backfill,
                        metadata = metadata,
                        embeddedResources =
                            resourceWrapper.embeddedResources.map { embedded ->
                                InteropResourcePublishV1.EmbeddedResource(
                                    resourceType = ResourceType.valueOf(embedded.resourceType),
                                    resourceJson = objectMapper.writeValueAsString(embedded),
                                )
                            },
                    )
                },
            )

        return listOf(nightlyTopic, adHocTopic, backfillTopic)
    }
}
