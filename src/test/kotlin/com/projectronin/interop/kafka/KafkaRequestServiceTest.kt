package com.projectronin.interop.kafka

import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.event.interop.resource.request.v1.InteropResourceRequestV1
import com.projectronin.interop.kafka.client.KafkaClient
import com.projectronin.interop.kafka.model.Failure
import com.projectronin.interop.kafka.model.KafkaAction
import com.projectronin.interop.kafka.model.KafkaEvent
import com.projectronin.interop.kafka.model.PushResponse
import com.projectronin.interop.kafka.model.RequestTopic
import com.projectronin.kafka.data.RoninEvent
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

class KafkaRequestServiceTest {
    private val topic = mockk<RequestTopic> {
        every { systemName } returns "interop-platform"
    }

    private val kafkaClient = mockk<KafkaClient>()
    private val tenantId = "test"
    private val service = KafkaRequestService(kafkaClient, topic)

    @Test
    fun `push single event success`() {
        val requestData = InteropResourceRequestV1(
            tenantId = tenantId,
            resourceFHIRId = "1234",
            resourceType = "PATIENT",
            requestingService = "testing-service"
        )
        val event = KafkaEvent("interop-platform", "resource", KafkaAction.REQUEST, "1234", data = requestData)
        every {
            kafkaClient.publishEvents(
                topic,
                listOf(event)
            )
        } returns PushResponse(successful = listOf(event))

        assertDoesNotThrow {
            service.pushRequestEvent(
                tenantId,
                listOf("1234"),
                ResourceType.Patient,
                "testing-service"
            )
        }
    }

    @Test
    fun `push single event failure`() {
        val requestData = InteropResourceRequestV1(
            tenantId = tenantId,
            resourceFHIRId = "1234",
            resourceType = "PATIENT",
            requestingService = "testing-service"
        )
        val event = KafkaEvent("interop-platform", "resource", KafkaAction.REQUEST, "1234", data = requestData)
        every {
            kafkaClient.publishEvents(
                topic,
                listOf(event)
            )
        } returns PushResponse(failures = listOf(Failure((event), error = Exception())))

        assertDoesNotThrow {
            service.pushRequestEvent(
                tenantId,
                listOf("1234"),
                ResourceType.Patient,
                "testing-service"
            )
        }

        assertEquals(
            service.pushRequestEvent(
                tenantId,
                listOf("1234"),
                ResourceType.Patient,
                "testing-service"
            )
                .failures.size,
            1
        )
    }

    @Test
    fun `push single event exception`() {
        val requestData = InteropResourceRequestV1(
            tenantId = tenantId,
            resourceFHIRId = "1234",
            resourceType = "PATIENT",
            requestingService = "testing-service"
        )
        val event = KafkaEvent("interop-platform", "resource", KafkaAction.REQUEST, "1234", data = requestData)
        every {
            kafkaClient.publishEvents(
                topic,
                listOf(event)
            )
        } throws Exception("error")

        assertEquals(
            service.pushRequestEvent(
                tenantId,
                listOf("1234"),
                ResourceType.Patient,
                "testing-service"
            ).failures.size,
            1
        )
    }

    @Test
    fun `retrieve events works`() {
        val requestData = InteropResourceRequestV1(
            tenantId = tenantId,
            resourceFHIRId = "1234",
            resourceType = "PATIENT",
            requestingService = "test-service"
        )
        every {
            kafkaClient.retrieveEvents(any(), any())
        } returns listOf(mockk { every { data } returns requestData })
        val ret = service.retrieveRequestEvents()
        assertEquals(requestData, ret.first())
    }

    @Test
    fun `retrieve events empty`() {
        every {
            kafkaClient.retrieveEvents(any(), any())
        } returns emptyList<RoninEvent<InteropResourceRequestV1>>()
        val ret = service.retrieveRequestEvents()
        assertEquals(emptyList<InteropResourceRequestV1>(), ret)
    }

    @Test
    fun `retrieve events works with new group ID`() {
        val requestData = InteropResourceRequestV1(
            tenantId = tenantId,
            resourceFHIRId = "1234",
            resourceType = "PATIENT",
            requestingService = "test-service"
        )
        every {
            kafkaClient.retrieveEvents(any(), any(), "override")
        } returns listOf(mockk { every { data } returns requestData })
        val ret = service.retrieveRequestEvents("override")
        assertEquals(requestData, ret.first())
    }
}
