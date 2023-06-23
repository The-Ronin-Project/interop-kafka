package com.projectronin.interop.kafka.spring

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TopicConfigTest {
    private val mockProperties = mockk<KafkaConfig> {
        every { cloud } returns mockk {
            every { region } returns "black-mesa-1"
            every { vendor } returns "bmrf-cloud"
        }
        every { retrieve } returns mockk {
            every { serviceId } returns "anti-mass-spec-service"
        }
    }

    @Test
    fun `get topics`() {
        val supportedResources = listOf(
            "Patient",
            "Binary",
            "Practitioner",
            "Appointment",
            "CarePlan",
            "CareTeam",
            "Communication",
            "Condition",
            "DocumentReference",
            "Encounter",
            "Location",
            "Medication",
            "MedicationRequest",
            "MedicationStatement",
            "Observation",
            "Organization",
            "PractitionerRole",
            "RequestGroup"
        )
        val numberOfResources = supportedResources.size
        assertEquals(numberOfResources, LoadSpringConfig(mockProperties).loadTopics().size)
        assertEquals(2 * numberOfResources, PublishSpringConfig(mockProperties).publishTopics().size)
    }

    @Test
    fun `topic names generate appropriately`() {
        val loadTopic = LoadSpringConfig(mockProperties).loadTopics().first()
        val publishTopic = PublishSpringConfig(mockProperties).publishTopics().first()
        val requestTopic = RequestSpringConfig(mockProperties).requestTopic()

        val expectedLoadTopicName = "bmrf-cloud.black-mesa-1.interop-mirth.patient-load.v1"
        assertEquals(expectedLoadTopicName, loadTopic.topicName)
        assertEquals("anti-mass-spec-service", loadTopic.systemName)

        val expectedPublishTopicName = "bmrf-cloud.black-mesa-1.interop-mirth.patient-publish-nightly.v1"
        assertEquals(expectedPublishTopicName, publishTopic.topicName)
        assertEquals("anti-mass-spec-service", publishTopic.systemName)

        val expectedRequestTopicName = "bmrf-cloud.black-mesa-1.interop-mirth.resource-request.v1"
        assertEquals(expectedRequestTopicName, requestTopic.topicName)
        assertEquals("anti-mass-spec-service", requestTopic.systemName)
    }
}
