package com.projectronin.interop.kafka.spring

import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.event.interop.internal.v1.eventName
import com.projectronin.interop.kafka.model.LoadTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class LoadSpringConfig(private val kafkaSpringConfig: KafkaConfig) {

    @Bean
    fun loadTopics(): List<LoadTopic> {
        val supportedResources = listOf(
            ResourceType.Patient,
            ResourceType.Binary,
            ResourceType.Practitioner,
            ResourceType.Appointment,
            ResourceType.CarePlan,
            ResourceType.CareTeam,
            ResourceType.Communication,
            ResourceType.Condition,
            ResourceType.Encounter,
            ResourceType.DocumentReference,
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
            ResourceType.RequestGroup
        )
        return supportedResources.map {
            generateTopics(it)
        }
    }

    fun generateTopics(resourceType: ResourceType): LoadTopic {
        val topicParameters = listOf(
            kafkaSpringConfig.cloud.vendor,
            kafkaSpringConfig.cloud.region,
            "interop-mirth",
            "${resourceType.eventName()}-load",
            "v1"
        )
        return LoadTopic(
            systemName = kafkaSpringConfig.retrieve.serviceId,
            topicName = topicParameters.joinToString("."),
            dataSchema = "https://github.com/projectronin/contract-event-interop-resource-load/blob/main/v1/resource-load-v1.schema.json",
            resourceType = resourceType
        )
    }
}
