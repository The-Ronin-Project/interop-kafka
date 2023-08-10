package com.projectronin.interop.kafka

import com.projectronin.event.interop.internal.v1.Metadata
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.interop.common.jackson.JacksonUtil
import com.projectronin.interop.fhir.r4.datatype.HumanName
import com.projectronin.interop.fhir.r4.datatype.Reference
import com.projectronin.interop.fhir.r4.datatype.primitive.Code
import com.projectronin.interop.fhir.r4.datatype.primitive.Date
import com.projectronin.interop.fhir.r4.datatype.primitive.Id
import com.projectronin.interop.fhir.r4.datatype.primitive.asFHIR
import com.projectronin.interop.fhir.r4.resource.Appointment
import com.projectronin.interop.fhir.r4.resource.Participant
import com.projectronin.interop.fhir.r4.resource.Patient
import com.projectronin.interop.fhir.r4.valueset.AdministrativeGender
import com.projectronin.interop.fhir.util.asCode
import com.projectronin.interop.kafka.client.KafkaClient
import com.projectronin.interop.kafka.model.DataTrigger
import com.projectronin.interop.kafka.spring.PublishSpringConfig
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class KafkaPublishServiceIT : BaseKafkaIT() {
    private val topics = PublishSpringConfig(kafkaConfig).publishTopics()
    private val kafkaClient = KafkaClient(kafkaConfig, kafkaAdmin)
    private val publishService = KafkaPublishService(kafkaClient, topics)

    @Disabled
    @Test
    fun `handles absurd number of events`() {
        val patientList = mutableListOf<Patient>()
        while (patientList.size < 300000) {
            patientList.add(
                Patient(
                    id = Id(patientList.size.toString()),
                    name = listOf(
                        HumanName(
                            family = "Public".asFHIR(),
                            given = listOf("John", "Q").asFHIR()
                        )
                    ),
                    gender = AdministrativeGender.MALE.asCode(),
                    birthDate = Date("1975-07-05")
                )
            )
        }
        val metadata = Metadata(runId = UUID.randomUUID().toString(), runDateTime = OffsetDateTime.now())

        val response =
            patientList.chunked(100000) { publishService.publishResources(tenantId, DataTrigger.AD_HOC, it, metadata) }
        assertEquals(0, response[0].failures.size)

        val publishedEvents = publishService.retrievePublishEvents(ResourceType.Patient, DataTrigger.AD_HOC)
        KotlinLogging.logger { }.info { patientList.size }
        assertEquals(300000, publishedEvents.size)
        assertEquals(metadata, publishedEvents.first().metadata)
    }

    @Test
    fun `can publish a single resource`() {
        val patient = Patient(
            id = Id("12345"),
            name = listOf(
                HumanName(
                    family = "Public".asFHIR(),
                    given = listOf("John", "Q").asFHIR()
                )
            ),
            gender = AdministrativeGender.MALE.asCode(),
            birthDate = Date("1975-07-05")
        )
        val metadata = Metadata(runId = UUID.randomUUID().toString(), runDateTime = OffsetDateTime.now())

        // because we start retrieving events from the latest we need to register the consumer,
        // that requires the topic is created, which is easiest-ly done by publishing a resource
        // so hence these two throw away lines to just get the consumer in a state where it'll receive a message
        publishService.publishResources(tenantId, DataTrigger.AD_HOC, listOf(patient), metadata)
        publishService.retrievePublishEvents(ResourceType.Patient, DataTrigger.AD_HOC)

        val response = publishService.publishResources(tenantId, DataTrigger.AD_HOC, listOf(patient), metadata)
        assertEquals(1, response.successful.size)
        assertEquals(patient, response.successful[0])

        assertEquals(0, response.failures.size)

        val publishedEvents = publishService.retrievePublishEvents(ResourceType.Patient, DataTrigger.AD_HOC)
        assertEquals(1, publishedEvents.size)
        assertEquals(JacksonUtil.writeJsonValue(patient), publishedEvents.first().resourceJson)
    }

    @Test
    @Disabled
    fun `can delete a topic`() {
        val patient = Patient(
            id = Id("12345"),
            name = listOf(
                HumanName(
                    family = "Public".asFHIR(),
                    given = listOf("John", "Q").asFHIR()
                )
            ),
            gender = AdministrativeGender.MALE.asCode(),
            birthDate = Date("1975-07-05")
        )
        val metadata = Metadata(runId = UUID.randomUUID().toString(), runDateTime = OffsetDateTime.now())

        val response = publishService.publishResources(tenantId, DataTrigger.AD_HOC, listOf(patient), metadata)
        assertEquals(1, response.successful.size)
        assertEquals(patient, response.successful[0])

        assertEquals(0, response.failures.size)
        publishService.deleteAllPublishTopics()
        val publishedEvents = publishService.retrievePublishEvents(ResourceType.Patient, DataTrigger.AD_HOC)
        assertEquals(0, publishedEvents.size)
    }

    @Test
    fun `can clear event`() {
        val patient = Patient(
            id = Id("12345"),
            name = listOf(
                HumanName(
                    family = "Public".asFHIR(),
                    given = listOf("John", "Q").asFHIR()
                )
            ),
            gender = AdministrativeGender.MALE.asCode(),
            birthDate = Date("1975-07-05")
        )
        val metadata = Metadata(runId = UUID.randomUUID().toString(), runDateTime = OffsetDateTime.now())
        val response = publishService.publishResources(tenantId, DataTrigger.AD_HOC, listOf(patient), metadata)
        assertEquals(1, response.successful.size)
        assertEquals(patient, response.successful[0])

        assertEquals(0, response.failures.size)
        publishService.retrievePublishEvents(ResourceType.Patient, DataTrigger.AD_HOC, null, true)
        val publishedEvents = publishService.retrievePublishEvents(ResourceType.Patient, DataTrigger.AD_HOC)
        assertEquals(0, publishedEvents.size)
    }

    @Test
    fun `can publish multiple resources of same type`() {
        val patient1 = Patient(
            id = Id("12345"),
            name = listOf(
                HumanName(
                    family = "Public".asFHIR(),
                    given = listOf("John", "Q").asFHIR()
                )
            ),
            gender = AdministrativeGender.MALE.asCode(),
            birthDate = Date("1975-07-05")
        )
        val patient2 = Patient(
            id = Id("67890"),
            name = listOf(
                HumanName(
                    family = "Doe".asFHIR(),
                    given = listOf("Jane").asFHIR()
                )
            ),
            gender = AdministrativeGender.FEMALE.asCode(),
            birthDate = Date("1975-07-05")
        )
        val metadata = Metadata(runId = UUID.randomUUID().toString(), runDateTime = OffsetDateTime.now())
        val response =
            publishService.publishResources(tenantId, DataTrigger.AD_HOC, listOf(patient1, patient2), metadata)
        assertEquals(2, response.successful.size)
        assertEquals(patient1, response.successful[0])
        assertEquals(patient2, response.successful[1])

        assertEquals(0, response.failures.size)
    }

    @Test
    fun `can publish multiple resources of differing type`() {
        val patient1 = Patient(
            id = Id("12345"),
            name = listOf(
                HumanName(
                    family = "Public".asFHIR(),
                    given = listOf("John", "Q").asFHIR()
                )
            ),
            gender = AdministrativeGender.MALE.asCode(),
            birthDate = Date("1975-07-05")
        )
        val appointment1 = Appointment(
            id = Id("67890"),
            participant = listOf(
                Participant(
                    actor = Reference(
                        reference = "Patient/12345".asFHIR()
                    ),
                    status = Code("accepted")
                )
            ),
            status = Code("fulfilled")
        )
        val metadata = Metadata(runId = UUID.randomUUID().toString(), runDateTime = OffsetDateTime.now())
        val response =
            publishService.publishResources(tenantId, DataTrigger.NIGHTLY, listOf(patient1, appointment1), metadata)
        assertEquals(2, response.successful.size)
        assertTrue(response.successful.contains(patient1))
        assertTrue(response.successful.contains(appointment1))

        assertEquals(0, response.failures.size)
    }
}
