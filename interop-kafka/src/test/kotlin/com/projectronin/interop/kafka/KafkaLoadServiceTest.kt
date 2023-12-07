package com.projectronin.interop.kafka

import com.projectronin.event.interop.internal.v1.InteropResourceLoadV1
import com.projectronin.event.interop.internal.v1.Metadata
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.interop.kafka.client.KafkaClient
import com.projectronin.interop.kafka.model.DataTrigger
import com.projectronin.interop.kafka.model.Failure
import com.projectronin.interop.kafka.model.KafkaAction
import com.projectronin.interop.kafka.model.KafkaEvent
import com.projectronin.interop.kafka.model.LoadTopic
import com.projectronin.interop.kafka.model.PushResponse
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime

class KafkaLoadServiceTest {
    private val patientTopic =
        mockk<LoadTopic> {
            every { resourceType } returns ResourceType.Patient
            every { systemName } returns "interop-platform"
        }

    private val appointmentTopic =
        mockk<LoadTopic> {
            every { resourceType } returns ResourceType.Appointment
            every { systemName } returns "interop-platform"
        }

    private val medicationRequestTopic =
        mockk<LoadTopic> {
            every { resourceType } returns ResourceType.MedicationRequest
            every { systemName } returns "interop-platform"
        }

    private val metadata = Metadata(runId = "testRun", runDateTime = OffsetDateTime.now())

    private val kafkaClient = mockk<KafkaClient>()
    private val tenantId = "test"
    private val service = KafkaLoadService(kafkaClient, listOf(patientTopic, appointmentTopic, medicationRequestTopic))

    @Test
    fun `publishing single resource is successful`() {
        val loadData =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
            )
        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.LOAD, "1234", data = loadData)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(successful = listOf(patientEvent))

        assertEquals(
            1,
            service.pushLoadEvent(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf("1234"),
                ResourceType.Patient,
                metadata,
            ).successful.size,
        )
    }

    @Test
    fun `publishing single resource with flow options is successful`() {
        val flowOptions =
            InteropResourceLoadV1.FlowOptions(
                disableDownstreamResources = true,
                normalizationRegistryMinimumTime = OffsetDateTime.now(),
            )
        val loadData =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
                flowOptions = flowOptions,
            )
        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.LOAD, "1234", data = loadData)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(successful = listOf(patientEvent))

        assertEquals(
            1,
            service.pushLoadEvent(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf("1234"),
                ResourceType.Patient,
                metadata,
                flowOptions,
            ).successful.size,
        )
    }

    @Test
    fun `publishing single resource failure`() {
        val loadData =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
            )
        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.LOAD, "1234", data = loadData)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } returns PushResponse(failures = listOf(Failure((patientEvent), error = Exception())))

        assertEquals(
            1,
            service.pushLoadEvent(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf("1234"),
                ResourceType.Patient,
                metadata,
            ).failures.size,
        )
    }

    @Test
    fun `publishing single resource exception`() {
        val loadData =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
            )
        val patientEvent = KafkaEvent("interop-platform", "patient", KafkaAction.LOAD, "1234", data = loadData)
        every {
            kafkaClient.publishEvents(
                patientTopic,
                listOf(patientEvent),
            )
        } throws Exception("error")

        assertEquals(
            1,
            service.pushLoadEvent(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf("1234"),
                ResourceType.Patient,
                metadata,
            ).failures.size,
        )
    }

    @Test
    fun `publishing single resource no topic`() {
        assertEquals(
            service.pushLoadEvent(
                tenantId,
                DataTrigger.NIGHTLY,
                listOf("1234"),
                ResourceType.Practitioner,
                metadata,
            ).failures.size,
            1,
        )
    }

    @Test
    fun `retrieve events works`() {
        val loadData =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
            )

        val flowOptions =
            InteropResourceLoadV1.FlowOptions(
                disableDownstreamResources = true,
            )
        val loadData2 =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
                flowOptions = flowOptions,
            )
        every { kafkaClient.retrieveEvents(any(), any()) } returns
            listOf(
                mockk { every { data } returns loadData },
                mockk { every { data } returns loadData2 },
            )
        val ret = service.retrieveLoadEvents(ResourceType.Patient)
        assertEquals(loadData, ret[0])
        assertEquals(loadData2, ret[1])
    }

    @Test
    fun `retrieve events empty`() {
        val loadData =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
            )
        every { kafkaClient.retrieveEvents(any(), any()) } returns listOf(mockk { every { data } returns loadData })
        val ret = service.retrieveLoadEvents(ResourceType.Practitioner)
        assertEquals(emptyList<InteropResourceLoadV1>(), ret)
    }

    @Test
    fun `retrieve events works with new group ID`() {
        val loadData =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.Patient,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
            )
        every {
            kafkaClient.retrieveEvents(
                any(),
                any(),
                "override",
            )
        } returns listOf(mockk { every { data } returns loadData })
        val ret = service.retrieveLoadEvents(ResourceType.Patient, "override")
        assertEquals(loadData, ret.first())
    }

    @Test
    fun `retrieve events removes special characters`() {
        val loadData =
            InteropResourceLoadV1(
                tenantId = tenantId,
                resourceFHIRId = "1234",
                resourceType = ResourceType.MedicationRequest,
                dataTrigger = InteropResourceLoadV1.DataTrigger.nightly,
                metadata = metadata,
            )
        every {
            kafkaClient.retrieveEvents(
                any(),
                any(),
                "any",
            )
        } returns listOf(mockk { every { data } returns loadData })
        val ret = service.retrieveLoadEvents(ResourceType.MedicationRequest, "any")
        assertEquals(loadData, ret.first())
    }
}
