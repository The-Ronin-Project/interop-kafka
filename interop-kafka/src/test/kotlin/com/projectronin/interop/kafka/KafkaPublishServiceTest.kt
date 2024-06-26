package com.projectronin.interop.kafka

import com.projectronin.event.interop.internal.v1.InteropResourcePublishV1
import com.projectronin.event.interop.internal.v1.Metadata
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.interop.fhir.r4.datatype.primitive.Id
import com.projectronin.interop.fhir.r4.resource.Appointment
import com.projectronin.interop.fhir.r4.resource.Organization
import com.projectronin.interop.fhir.r4.resource.Patient
import com.projectronin.interop.fhir.r4.resource.Practitioner
import com.projectronin.interop.kafka.client.KafkaClient
import com.projectronin.interop.kafka.model.DataTrigger
import com.projectronin.interop.kafka.model.Failure
import com.projectronin.interop.kafka.model.KafkaAction
import com.projectronin.interop.kafka.model.KafkaEvent
import com.projectronin.interop.kafka.model.PublishResourceWrapper
import com.projectronin.interop.kafka.model.PublishTopic
import com.projectronin.interop.kafka.model.PushResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime

class KafkaPublishServiceTest {
    private val selfConverter = { _: String, resourceWrapper: PublishResourceWrapper, _: Metadata -> resourceWrapper }

    private val patientTopic =
        mockk<PublishTopic> {
            every { resourceType } returns ResourceType.Patient
            every { systemName } returns "interop-platform"
            every { converter } returns selfConverter
            every { dataTrigger } returns DataTrigger.NIGHTLY
        }

    private val appointmentTopic =
        mockk<PublishTopic> {
            every { resourceType } returns ResourceType.Appointment
            every { systemName } returns "interop-platform"
            every { converter } returns selfConverter
            every { dataTrigger } returns DataTrigger.NIGHTLY
        }

    private val medicationRequestTopic =
        mockk<PublishTopic> {
            every { resourceType } returns ResourceType.MedicationRequest
            every { systemName } returns "interop-platform"
            every { converter } returns selfConverter
            every { dataTrigger } returns DataTrigger.NIGHTLY
        }

    private val kafkaClient = mockk<KafkaClient>()
    private val tenantId = "test"
    private val metadata = Metadata(runId = "testRun", runDateTime = OffsetDateTime.now())
    private val service =
        KafkaPublishService(kafkaClient, listOf(patientTopic, appointmentTopic, medicationRequestTopic))

    @Test
    fun `publishResources handles successes and failures`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val appointment =
            mockk<Appointment> {
                every { resourceType } returns "Appointment"
                every { id } returns Id("5678")
            }

        val patientEvent =
            KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", PublishResourceWrapper(patient))
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(successful = listOf(patientEvent))

        val appointmentEvent =
            KafkaEvent(
                "interop-platform",
                "appointment",
                KafkaAction.PUBLISH,
                "5678",
                PublishResourceWrapper(appointment),
            )
        every {
            kafkaClient.publishEvents(
                appointmentTopic,
                listOf(appointmentEvent),
            )
        } throws IllegalStateException("exception")

        val response =
            service.publishResources(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf(patient, appointment),
                metadata,
            )
        assertEquals(1, response.successful.size)
        assertEquals(patient, response.successful[0])

        assertEquals(1, response.failures.size)
        assertEquals(appointment, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals("exception", response.failures[0].error.message)
    }

    @Test
    fun `publishing single resource is successful`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val patientWrapper = PublishResourceWrapper(patient)

        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(successful = listOf(patientEvent))

        val response = service.publishResourceWrappers(tenantId, DataTrigger.NIGHTLY, listOf(patientWrapper), metadata)
        assertEquals(1, response.successful.size)
        assertEquals(patientWrapper, response.successful[0])

        assertEquals(0, response.failures.size)
    }

    @Test
    fun `publishing single resource with embedded resources is successful`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val organization = mockk<Organization>()
        val practitioner = mockk<Practitioner>()
        val patientWrapper = PublishResourceWrapper(patient, listOf(organization, practitioner))

        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(successful = listOf(patientEvent))

        val response = service.publishResourceWrappers(tenantId, DataTrigger.NIGHTLY, listOf(patientWrapper), metadata)
        assertEquals(1, response.successful.size)
        assertEquals(patientWrapper, response.successful[0])

        assertEquals(0, response.failures.size)
    }

    @Test
    fun `publishing single resource has failure`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val patientWrapper = PublishResourceWrapper(patient)

        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(failures = listOf(Failure(patientEvent, IllegalStateException("exception"))))

        val response = service.publishResourceWrappers(tenantId, DataTrigger.NIGHTLY, listOf(patientWrapper), metadata)
        assertEquals(0, response.successful.size)

        assertEquals(1, response.failures.size)
        assertEquals(patientWrapper, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals("exception", response.failures[0].error.message)
    }

    @Test
    fun `publishing single resource with embedded resources has failure`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val organization = mockk<Organization>()
        val practitioner = mockk<Practitioner>()
        val patientWrapper = PublishResourceWrapper(patient, listOf(organization, practitioner))

        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(failures = listOf(Failure(patientEvent, IllegalStateException("exception"))))

        val response = service.publishResourceWrappers(tenantId, DataTrigger.NIGHTLY, listOf(patientWrapper), metadata)
        assertEquals(0, response.successful.size)

        assertEquals(1, response.failures.size)
        assertEquals(patientWrapper, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals("exception", response.failures[0].error.message)
    }

    @Test
    fun `publishing single resource throws exception`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val patientWrapper = PublishResourceWrapper(patient)

        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } throws IllegalStateException("exception")

        val response = service.publishResourceWrappers(tenantId, DataTrigger.NIGHTLY, listOf(patientWrapper), metadata)
        assertEquals(0, response.successful.size)

        assertEquals(1, response.failures.size)
        assertEquals(patientWrapper, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals("exception", response.failures[0].error.message)
    }

    @Test
    fun `publishing multiple resources of same type throws exception`() {
        val patient1 =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val patient2 =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("5678")
            }
        val patientWrapper1 = PublishResourceWrapper(patient1)
        val patientWrapper2 = PublishResourceWrapper(patient2)

        val patientEvent1 = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper1)
        val patientEvent2 = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "5678", patientWrapper2)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent1, patientEvent2),
            )
        } throws IllegalStateException("exception")

        val response =
            service.publishResourceWrappers(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf(patientWrapper1, patientWrapper2),
                metadata,
            )
        assertEquals(0, response.successful.size)

        assertEquals(2, response.failures.size)
        assertEquals(patientWrapper1, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals("exception", response.failures[0].error.message)

        assertEquals(patientWrapper2, response.failures[1].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[1].error)
        assertEquals("exception", response.failures[1].error.message)
    }

    @Test
    fun `some resources do not succeed when multiple supplied`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val appointment =
            mockk<Appointment> {
                every { resourceType } returns "Appointment"
                every { id } returns Id("5678")
            }
        val patientWrapper = PublishResourceWrapper(patient)
        val appointmentWrapper = PublishResourceWrapper(appointment)

        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(successful = listOf(patientEvent))

        val appointmentEvent =
            KafkaEvent("interop-platform", "appointment", KafkaAction.PUBLISH, "5678", appointmentWrapper)
        every {
            kafkaClient.publishEvents(
                appointmentTopic,
                listOf(appointmentEvent),
            )
        } throws IllegalStateException("exception")

        val response =
            service.publishResourceWrappers(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf(patientWrapper, appointmentWrapper),
                metadata,
            )
        assertEquals(1, response.successful.size)
        assertEquals(patientWrapper, response.successful[0])

        assertEquals(1, response.failures.size)
        assertEquals(appointmentWrapper, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals("exception", response.failures[0].error.message)
    }

    @Test
    fun `all resources publish when multiple supplied`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val appointment =
            mockk<Appointment> {
                every { resourceType } returns "Appointment"
                every { id } returns Id("5678")
            }
        val patientWrapper = PublishResourceWrapper(patient)
        val appointmentWrapper = PublishResourceWrapper(appointment)

        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(successful = listOf(patientEvent))

        val appointmentEvent =
            KafkaEvent("interop-platform", "appointment", KafkaAction.PUBLISH, "5678", appointmentWrapper)
        every {
            kafkaClient.publishEvents(
                appointmentTopic,
                listOf(appointmentEvent),
            )
        } returns PushResponse(successful = listOf(appointmentEvent))

        val response =
            service.publishResourceWrappers(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf(patientWrapper, appointmentWrapper),
                metadata,
            )
        assertEquals(2, response.successful.size)
        assertTrue(response.successful.contains(patientWrapper))
        assertTrue(response.successful.contains(appointmentWrapper))

        assertEquals(0, response.failures.size)
    }

    @Test
    fun `supports multiple resources of the same type`() {
        val patient1 =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val patient2 =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("3456")
            }
        val appointment1 =
            mockk<Appointment> {
                every { resourceType } returns "Appointment"
                every { id } returns Id("5678")
            }
        val appointment2 =
            mockk<Appointment> {
                every { resourceType } returns "Appointment"
                every { id } returns Id("7890")
            }
        val patientWrapper1 = PublishResourceWrapper(patient1)
        val patientWrapper2 = PublishResourceWrapper(patient2)
        val appointmentWrapper1 = PublishResourceWrapper(appointment1)
        val appointmentWrapper2 = PublishResourceWrapper(appointment2)

        val patientEvent1 = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper1)
        val patientEvent2 = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "3456", patientWrapper2)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent1, patientEvent2),
            )
        } returns PushResponse(successful = listOf(patientEvent1, patientEvent2))

        val appointmentEvent1 =
            KafkaEvent("interop-platform", "appointment", KafkaAction.PUBLISH, "5678", appointmentWrapper1)
        val appointmentEvent2 =
            KafkaEvent("interop-platform", "appointment", KafkaAction.PUBLISH, "7890", appointmentWrapper2)
        every {
            kafkaClient.publishEvents(
                appointmentTopic,
                listOf(appointmentEvent1, appointmentEvent2),
            )
        } returns PushResponse(successful = listOf(appointmentEvent1, appointmentEvent2))

        val response =
            service.publishResourceWrappers(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf(patientWrapper1, patientWrapper2, appointmentWrapper1, appointmentWrapper2),
                metadata,
            )
        assertEquals(4, response.successful.size)
        assertTrue(response.successful.contains(patientWrapper1))
        assertTrue(response.successful.contains(patientWrapper2))
        assertTrue(response.successful.contains(appointmentWrapper1))
        assertTrue(response.successful.contains(appointmentWrapper2))

        assertEquals(0, response.failures.size)
    }

    @Test
    fun `fails when trigger type does not match`() {
        val appointmentTopicAdhoc =
            mockk<PublishTopic> {
                every { resourceType } returns ResourceType.Appointment
                every { systemName } returns "interop-platform"
                every { converter } returns selfConverter
                every { dataTrigger } returns DataTrigger.AD_HOC
            }

        val serviceMixed = KafkaPublishService(kafkaClient, listOf(patientTopic, appointmentTopicAdhoc))

        val patient1 =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val patient2 =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("3456")
            }
        val appointment1 =
            mockk<Appointment> {
                every { resourceType } returns "Appointment"
                every { id } returns Id("5678")
            }
        val appointment2 =
            mockk<Appointment> {
                every { resourceType } returns "Appointment"
                every { id } returns Id("7890")
            }
        val patientWrapper1 = PublishResourceWrapper(patient1)
        val patientWrapper2 = PublishResourceWrapper(patient2)
        val appointmentWrapper1 = PublishResourceWrapper(appointment1)
        val appointmentWrapper2 = PublishResourceWrapper(appointment2)

        val patientEvent1 = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper1)
        val patientEvent2 = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "3456", patientWrapper2)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent1, patientEvent2),
            )
        } returns PushResponse(successful = listOf(patientEvent1, patientEvent2))

        val appointmentEvent1 =
            KafkaEvent("interop-platform", "appointment", KafkaAction.PUBLISH, "5678", appointmentWrapper1)
        val appointmentEvent2 =
            KafkaEvent("interop-platform", "appointment", KafkaAction.PUBLISH, "7890", appointmentWrapper2)
        every {
            kafkaClient.publishEvents(
                appointmentTopicAdhoc,
                listOf(appointmentEvent1, appointmentEvent2),
            )
        } returns PushResponse(successful = listOf(appointmentEvent1, appointmentEvent2))

        val response =
            serviceMixed.publishResourceWrappers(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf(patientWrapper1, patientWrapper2, appointmentWrapper1, appointmentWrapper2),
                metadata,
            )
        assertEquals(2, response.successful.size)
        assertTrue(response.successful.contains(patientWrapper1))
        assertTrue(response.successful.contains(patientWrapper2))

        assertEquals(2, response.failures.size)
    }

    @Test
    fun `no topic associated to resource type`() {
        val practitioner =
            mockk<Practitioner> {
                every { resourceType } returns "Practitioner"
                every { id } returns Id("1234")
            }
        val practitionerWrapper = PublishResourceWrapper(practitioner)

        val response =
            service.publishResourceWrappers(tenantId, DataTrigger.NIGHTLY, listOf(practitionerWrapper), metadata)
        assertEquals(0, response.successful.size)

        assertEquals(1, response.failures.size)
        assertEquals(practitionerWrapper, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals(
            "Zero or multiple PublishTopics associated to resource type Practitioner",
            response.failures[0].error.message,
        )

        verify(exactly = 0) { kafkaClient.publishEvents<Any>(any(), any()) }
    }

    @Test
    fun `no topic associated to resource type with multiple resources`() {
        val practitioner1 =
            mockk<Practitioner> {
                every { resourceType } returns "Practitioner"
                every { id } returns Id("1234")
            }
        val practitioner2 =
            mockk<Practitioner> {
                every { resourceType } returns "Practitioner"
                every { id } returns Id("5678")
            }
        val practitionerWrapper1 = PublishResourceWrapper(practitioner1)
        val practitionerWrapper2 = PublishResourceWrapper(practitioner2)

        val response =
            service.publishResourceWrappers(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf(practitionerWrapper1, practitionerWrapper2),
                metadata,
            )
        assertEquals(0, response.successful.size)

        assertEquals(2, response.failures.size)
        assertEquals(practitionerWrapper1, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals(
            "Zero or multiple PublishTopics associated to resource type Practitioner",
            response.failures[0].error.message,
        )

        assertEquals(practitionerWrapper2, response.failures[1].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[1].error)
        assertEquals(
            "Zero or multiple PublishTopics associated to resource type Practitioner",
            response.failures[1].error.message,
        )

        verify(exactly = 0) { kafkaClient.publishEvents<Any>(any(), any()) }
    }

    @Test
    fun `other resources published when no topic associated to one of supplied types`() {
        val patient =
            mockk<Patient> {
                every { resourceType } returns "Patient"
                every { id } returns Id("1234")
            }
        val practitioner =
            mockk<Practitioner> {
                every { resourceType } returns "Practitioner"
                every { id } returns Id("1234")
            }
        val patientWrapper = PublishResourceWrapper(patient)
        val practitionerWrapper = PublishResourceWrapper(practitioner)

        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.PUBLISH, "1234", patientWrapper)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(successful = listOf(patientEvent))

        val response =
            service.publishResourceWrappers(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf(patientWrapper, practitionerWrapper),
                metadata,
            )
        assertEquals(1, response.successful.size)
        assertEquals(patientWrapper, response.successful[0])

        assertEquals(1, response.failures.size)
        assertEquals(practitionerWrapper, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals(
            "Zero or multiple PublishTopics associated to resource type Practitioner",
            response.failures[0].error.message,
        )

        verify(exactly = 1) { kafkaClient.publishEvents<Any>(any(), any()) }
    }

    @Test
    fun `retrieve events works`() {
        val publishEvent =
            InteropResourcePublishV1(
                tenantId = tenantId,
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourcePublishV1.DataTrigger.nightly,
                resourceJson = "json",
                metadata = metadata,
            )
        every { kafkaClient.retrieveEvents(any(), any()) } returns listOf(mockk { every { data } returns publishEvent })
        val ret = service.retrievePublishEvents(ResourceType.Patient, DataTrigger.NIGHTLY)
        assertEquals(publishEvent, ret.first())
    }

    @Test
    fun `retrieve events works for just clear`() {
        val publishEvent =
            InteropResourcePublishV1(
                tenantId = tenantId,
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourcePublishV1.DataTrigger.nightly,
                resourceJson = "json",
                metadata = metadata,
            )
        every {
            kafkaClient.retrieveEvents(
                topic = any(),
                typeMap = any(),
                duration = any(),
                groupId = "123",
            )
        } returns listOf(mockk { every { data } returns publishEvent })
        val ret = service.retrievePublishEvents(ResourceType.Patient, DataTrigger.NIGHTLY, "123", true)
        assertEquals(0, ret.size)
    }

    @Test
    fun `retrieve events empty`() {
        val publishEvent =
            InteropResourcePublishV1(
                tenantId = tenantId,
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourcePublishV1.DataTrigger.nightly,
                resourceJson = "json",
                metadata = metadata,
            )
        every { kafkaClient.retrieveEvents(any(), any()) } returns listOf(mockk { every { data } returns publishEvent })
        val ret = service.retrievePublishEvents(ResourceType.Patient, DataTrigger.AD_HOC)
        assertEquals(emptyList<InteropResourcePublishV1>(), ret)
    }

    @Test
    fun `retrieve events works with new group ID`() {
        val publishEvent =
            InteropResourcePublishV1(
                tenantId = tenantId,
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourcePublishV1.DataTrigger.nightly,
                resourceJson = "json",
                metadata = metadata,
            )
        every {
            kafkaClient.retrieveEvents(
                any(),
                any(),
                "override",
            )
        } returns listOf(mockk { every { data } returns publishEvent })
        val ret = service.retrievePublishEvents(ResourceType.Patient, DataTrigger.NIGHTLY, "override")
        assertEquals(publishEvent, ret.first())
    }

    @Test
    fun `retrieve events removes special characters`() {
        val publishEvent =
            InteropResourcePublishV1(
                tenantId = tenantId,
                resourceType = ResourceType.MedicationRequest,
                dataTrigger = InteropResourcePublishV1.DataTrigger.nightly,
                resourceJson = "json",
                metadata = metadata,
            )
        every {
            kafkaClient.retrieveEvents(
                any(),
                any(),
                "any",
            )
        } returns listOf(mockk { every { data } returns publishEvent })
        val ret = service.retrievePublishEvents(ResourceType.MedicationRequest, DataTrigger.NIGHTLY, "any")
        assertEquals(publishEvent, ret.first())
    }
}
