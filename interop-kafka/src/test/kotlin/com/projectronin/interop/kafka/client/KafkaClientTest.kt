package com.projectronin.interop.kafka.client

import com.projectronin.event.interop.internal.v1.InteropResourcePublishV1
import com.projectronin.event.interop.internal.v1.Metadata
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.interop.kafka.model.KafkaEvent
import com.projectronin.interop.kafka.model.KafkaTopic
import com.projectronin.interop.kafka.spring.KafkaCloudConfig
import com.projectronin.interop.kafka.spring.KafkaConfig
import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import io.mockk.Runs
import io.mockk.every
import io.mockk.invoke
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkStatic
import io.mockk.unmockkAll
import io.mockk.unmockkStatic
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

class KafkaClientTest {
    private val cloudConfig =
        KafkaCloudConfig(
            vendor = "local",
            region = "local",
        )

    private val kafkaConfig =
        mockk<KafkaConfig> {
            every { cloud } returns cloudConfig
            every { publish.source } returns "source"
        }

    private val producersProperty =
        KafkaClient::class.memberProperties.find {
            it.name == "producersByTopicName"
        }!! as KProperty1<KafkaClient, MutableMap<String, RoninProducer>>

    @BeforeEach
    fun setup() {
        mockkConstructor(RoninProducer::class)
        mockkStatic("com.projectronin.interop.kafka.client.KafkaUtilsKt")

        producersProperty.isAccessible = true
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    @Test
    fun `creates new producer when none exist for topic`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>> {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
                every { patientId } returns null
                every { tenantId } returns null
                every { resourceVersionId } returns null
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } returns mockk()
                    }
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)

        val preCallProducers = producersProperty.get(client)
        assertTrue(preCallProducers.isEmpty())

        val response = client.publishEvents(topic, listOf(event1))
        assertEquals(1, response.successful.size)
        assertEquals(event1, response.successful[0])

        assertEquals(0, response.failures.size)

        val postCallProducers = producersProperty.get(client)
        assertEquals(1, postCallProducers.size)
        assertEquals(producer, postCallProducers["test.topic.name"])

        verify(exactly = 1) { createProducer(topic, kafkaConfig) }
        verify(exactly = 1) {
            producer.send(
                withArg<RoninEvent<String>> {
                    assertEquals("test.topic.name.schema", it.dataSchema)
                    assertEquals("source", it.source)
                    assertEquals("event.type", it.type)
                    assertEquals("Data 1", it.data)
                    assertEquals("event", it.resourceType)
                    assertEquals("1", it.resourceId)
                    assertNull(it.patientId)
                    assertNull(it.tenantId)
                    assertNull(it.resourceVersion)
                },
            )
        }
    }

    @Test
    fun `reuses producer when one exists for topic`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>> {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
                every { patientId } returns null
                every { tenantId } returns null
                every { resourceVersionId } returns null
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } returns mockk()
                    }
            }

        val client = KafkaClient(kafkaConfig)

        val preCallProducers = producersProperty.get(client)
        assertEquals(0, preCallProducers.size)

        preCallProducers["test.topic.name"] = producer

        val response = client.publishEvents(topic, listOf(event1))
        assertEquals(1, response.successful.size)
        assertEquals(event1, response.successful[0])

        assertEquals(0, response.failures.size)

        val postCallProducers = producersProperty.get(client)
        assertEquals(1, postCallProducers.size)
        assertEquals(producer, postCallProducers["test.topic.name"])

        verify(exactly = 0) { createProducer(topic, kafkaConfig) }
        verify(exactly = 1) {
            producer.send(
                withArg<RoninEvent<String>> {
                    assertEquals("test.topic.name.schema", it.dataSchema)
                    assertEquals("source", it.source)
                    assertEquals("event.type", it.type)
                    assertEquals("Data 1", it.data)
                    assertEquals("event", it.resourceType)
                    assertEquals("1", it.resourceId)
                    assertNull(it.patientId)
                    assertNull(it.tenantId)
                    assertNull(it.resourceVersion)
                },
            )
        }
    }

    @Test
    fun `handles send that results in an exception`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>> {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
                every { patientId } returns null
                every { tenantId } returns null
                every { resourceVersionId } returns null
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } throws IllegalStateException("exception")
                    }
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)
        val response = client.publishEvents(topic, listOf(event1))
        assertEquals(0, response.successful.size)

        assertEquals(1, response.failures.size)
        assertEquals(event1, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals("exception", response.failures[0].error.message)

        verify(exactly = 1) { createProducer(topic, kafkaConfig) }
        verify(exactly = 1) {
            producer.send(
                withArg<RoninEvent<String>> {
                    assertEquals("test.topic.name.schema", it.dataSchema)
                    assertEquals("source", it.source)
                    assertEquals("event.type", it.type)
                    assertEquals("Data 1", it.data)
                    assertEquals("event", it.resourceType)
                    assertEquals("1", it.resourceId)
                    assertNull(it.patientId)
                    assertNull(it.tenantId)
                    assertNull(it.resourceVersion)
                },
            )
        }
    }

    @Test
    fun `handles send that results in success`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>> {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
                every { patientId } returns null
                every { tenantId } returns null
                every { resourceVersionId } returns null
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } returns mockk()
                    }
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)
        val response = client.publishEvents(topic, listOf(event1))
        assertEquals(1, response.successful.size)
        assertEquals(event1, response.successful[0])

        assertEquals(0, response.failures.size)

        verify(exactly = 1) { createProducer(topic, kafkaConfig) }
        verify(exactly = 1) {
            producer.send(
                withArg<RoninEvent<String>> {
                    assertEquals("test.topic.name.schema", it.dataSchema)
                    assertEquals("source", it.source)
                    assertEquals("event.type", it.type)
                    assertEquals("Data 1", it.data)
                    assertEquals("event", it.resourceType)
                    assertEquals("1", it.resourceId)
                    assertNull(it.patientId)
                    assertNull(it.tenantId)
                    assertNull(it.resourceVersion)
                },
            )
        }
    }

    @Test
    fun `handles event with patientId`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>> {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
                every { patientId } returns "12345"
                every { tenantId } returns null
                every { resourceVersionId } returns null
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } returns mockk()
                    }
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)
        val response = client.publishEvents(topic, listOf(event1))
        assertEquals(1, response.successful.size)
        assertEquals(event1, response.successful[0])

        assertEquals(0, response.failures.size)

        verify(exactly = 1) { createProducer(topic, kafkaConfig) }
        verify(exactly = 1) {
            producer.send(
                withArg<RoninEvent<String>> {
                    assertEquals("test.topic.name.schema", it.dataSchema)
                    assertEquals("source", it.source)
                    assertEquals("event.type", it.type)
                    assertEquals("Data 1", it.data)
                    assertEquals("event", it.resourceType)
                    assertEquals("1", it.resourceId)
                    assertEquals("12345", it.patientId)
                    assertNull(it.tenantId)
                    assertNull(it.resourceVersion)
                },
            )
        }
    }

    @Test
    fun `handles event with tenantId`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>> {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
                every { patientId } returns null
                every { tenantId } returns "tenant"
                every { resourceVersionId } returns null
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } returns mockk()
                    }
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)
        val response = client.publishEvents(topic, listOf(event1))
        assertEquals(1, response.successful.size)
        assertEquals(event1, response.successful[0])

        assertEquals(0, response.failures.size)

        verify(exactly = 1) { createProducer(topic, kafkaConfig) }
        verify(exactly = 1) {
            producer.send(
                withArg<RoninEvent<String>> {
                    assertEquals("test.topic.name.schema", it.dataSchema)
                    assertEquals("source", it.source)
                    assertEquals("event.type", it.type)
                    assertEquals("Data 1", it.data)
                    assertEquals("event", it.resourceType)
                    assertEquals("1", it.resourceId)
                    assertNull(it.patientId)
                    assertEquals("tenant", it.tenantId)
                    assertNull(it.resourceVersion)
                },
            )
        }
    }

    @Test
    fun `handles event with resourceVersionId`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>> {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
                every { patientId } returns null
                every { tenantId } returns null
                every { resourceVersionId } returns 368
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } returns mockk()
                    }
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)
        val response = client.publishEvents(topic, listOf(event1))
        assertEquals(1, response.successful.size)
        assertEquals(event1, response.successful[0])

        assertEquals(0, response.failures.size)

        verify(exactly = 1) { createProducer(topic, kafkaConfig) }
        verify(exactly = 1) {
            producer.send(
                withArg<RoninEvent<String>> {
                    assertEquals("test.topic.name.schema", it.dataSchema)
                    assertEquals("source", it.source)
                    assertEquals("event.type", it.type)
                    assertEquals("Data 1", it.data)
                    assertEquals("event", it.resourceType)
                    assertEquals("1", it.resourceId)
                    assertNull(it.patientId)
                    assertNull(it.tenantId)
                    assertEquals(368, it.resourceVersion)
                },
            )
        }
    }

    @Test
    fun `handles non-null trigger`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name-nightly"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>> {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
                every { patientId } returns null
                every { tenantId } returns null
                every { resourceVersionId } returns null
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } returns mockk()
                    }
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)
        val response = client.publishEvents(topic, listOf(event1))
        assertEquals(1, response.successful.size)
        assertEquals(event1, response.successful[0])

        assertEquals(0, response.failures.size)

        verify(exactly = 1) { createProducer(topic, kafkaConfig) }
        verify(exactly = 1) {
            producer.send(
                withArg<RoninEvent<String>> {
                    assertEquals("test.topic.name.schema", it.dataSchema)
                    assertEquals("source", it.source)
                    assertEquals("event.type", it.type)
                    assertEquals("Data 1", it.data)
                    assertEquals("event", it.resourceType)
                    assertEquals("1", it.resourceId)
                    assertNull(it.patientId)
                    assertNull(it.tenantId)
                    assertNull(it.resourceVersion)
                },
            )
        }
    }

    @Test
    fun `handles some results are failure`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>>(relaxed = true) {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
            }
        val event2 =
            mockk<KafkaEvent<String>>(relaxed = true) {
                every { type } returns "event.type"
                every { subject } returns "event/2"
                every { data } returns "Data 2"
            }
        val event3 =
            mockk<KafkaEvent<String>>(relaxed = true) {
                every { type } returns "event.type"
                every { subject } returns "event/3"
                every { data } returns "Data 3"
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returnsMany
                    listOf(
                        mockk {
                            every { get() } returns mockk()
                        },
                        mockk {
                            every { get() } throws IllegalStateException("exception")
                        },
                        mockk {
                            every { get() } returns mockk()
                        },
                    )
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)
        val response = client.publishEvents(topic, listOf(event1, event2, event3))
        assertEquals(2, response.successful.size)
        assertEquals(event1, response.successful[0])
        assertEquals(event3, response.successful[1])

        assertEquals(1, response.failures.size)
        assertEquals(event2, response.failures[0].data)
        assertInstanceOf(IllegalStateException::class.java, response.failures[0].error)
        assertEquals("exception", response.failures[0].error.message)
    }

    @Test
    fun `handles all results are success`() {
        val topic =
            mockk<KafkaTopic> {
                every { topicName } returns "test.topic.name"
                every { dataSchema } returns "test.topic.name.schema"
            }
        val event1 =
            mockk<KafkaEvent<String>>(relaxed = true) {
                every { type } returns "event.type"
                every { subject } returns "event/1"
                every { data } returns "Data 1"
            }
        val event2 =
            mockk<KafkaEvent<String>>(relaxed = true) {
                every { type } returns "event.type"
                every { subject } returns "event/2"
                every { data } returns "Data 2"
            }
        val event3 =
            mockk<KafkaEvent<String>>(relaxed = true) {
                every { type } returns "event.type"
                every { subject } returns "event/3"
                every { data } returns "Data 3"
            }

        val producer =
            mockk<RoninProducer> {
                every { send(any<RoninEvent<String>>()) } returns
                    mockk {
                        every { get() } returns mockk()
                    }
            }

        every { createProducer(topic, kafkaConfig) } returns producer

        val client = KafkaClient(kafkaConfig)
        val response = client.publishEvents(topic, listOf(event1, event2, event3))
        assertEquals(3, response.successful.size)
        assertEquals(event1, response.successful[0])
        assertEquals(event2, response.successful[1])
        assertEquals(event3, response.successful[2])

        assertEquals(0, response.failures.size)
    }

    @Test
    fun `retrieve events works`() {
        val mockEvent = mockk<RoninEvent<InteropResourcePublishV1>>()
        every { mockEvent.data } returns
            InteropResourcePublishV1(
                "TENANT",
                ResourceType.Patient,
                InteropResourcePublishV1.DataTrigger.nightly,
                resourceJson = "json",
                metadata = Metadata(runId = "1234", runDateTime = OffsetDateTime.now()),
            )
        every { mockEvent.id } returns "messageID"
        mockkStatic(::createConsumer)
        val mockConsumer = mockk<RoninConsumer>()
        every { createConsumer(any(), any(), any()) } returns mockConsumer
        every { mockConsumer.pollOnce(timeout = any(), handler = captureLambda()) } answers {
            lambda<(RoninEvent<*>) -> RoninEventResult>().invoke(mockEvent)
        }
        every { mockConsumer.stop() } just Runs
        every { mockConsumer.unsubscribe() } just Runs
        val client = KafkaClient(kafkaConfig)
        val ret =
            client.retrieveEvents(
                topic = mockk { every { topicName } returns "topicName" },
                typeMap = mapOf(),
                limit = 1,
            )
        assertEquals(ret.size, 1)
        unmockkStatic(::createConsumer)
    }

    @Test
    fun `retrieve multi events works`() {
        val mockEvent = mockk<RoninEvent<InteropResourcePublishV1>>()
        every { mockEvent.data } returns
            InteropResourcePublishV1(
                "TENANT",
                ResourceType.Patient,
                InteropResourcePublishV1.DataTrigger.nightly,
                resourceJson = "json",
                metadata = Metadata(runId = "1234", runDateTime = OffsetDateTime.now()),
            )
        every { mockEvent.id } returns "messageID"
        mockkStatic(::createMultiConsumer)
        val mockConsumer = mockk<RoninConsumer>()
        every { createMultiConsumer(any(), any(), any(), any()) } returns mockConsumer
        every { mockConsumer.pollOnce(timeout = any(), handler = captureLambda()) } answers {
            lambda<(RoninEvent<*>) -> RoninEventResult>().invoke(mockEvent)
        }
        every { mockConsumer.stop() } just Runs
        every { mockConsumer.unsubscribe() } just Runs
        val client = KafkaClient(kafkaConfig)
        val ret =
            client.retrieveMultiTopicEvents(
                topicList = listOf(mockk { every { topicName } returns "topicName" }),
                typeMap = mapOf(),
                groupId = "groupID",
                limit = 1,
            )
        assertEquals(ret.size, 1)
        unmockkStatic(::createMultiConsumer)
    }

    @Test
    fun `retrieve events works with overridden group`() {
        val mockEvent = mockk<RoninEvent<InteropResourcePublishV1>>()
        every { mockEvent.data } returns
            InteropResourcePublishV1(
                "TENANT",
                ResourceType.Patient,
                InteropResourcePublishV1.DataTrigger.nightly,
                resourceJson = "json",
                metadata = Metadata(runId = "1234", runDateTime = OffsetDateTime.now()),
            )
        every { mockEvent.id } returns "messageID"
        mockkStatic(::createConsumer)
        val mockConsumer = mockk<RoninConsumer>()
        every { createConsumer(any(), any(), any(), "override!") } returns mockConsumer
        every { mockConsumer.pollOnce(timeout = any(), handler = captureLambda()) } answers {
            lambda<(RoninEvent<*>) -> RoninEventResult>().invoke(mockEvent)
        }
        every { mockConsumer.stop() } just Runs
        every { mockConsumer.unsubscribe() } just Runs
        val client = KafkaClient(kafkaConfig)
        val ret =
            client.retrieveEvents(
                topic = mockk { every { topicName } returns "topicName" },
                typeMap = mapOf(),
                groupId = "override!",
                limit = 1,
            )
        assertEquals(ret.size, 1)
        unmockkStatic(::createConsumer)
    }
}
