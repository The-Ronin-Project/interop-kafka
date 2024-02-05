package com.projectronin.interop.kafka.client

import com.projectronin.interop.kafka.model.Failure
import com.projectronin.interop.kafka.model.KafkaEvent
import com.projectronin.interop.kafka.model.KafkaTopic
import com.projectronin.interop.kafka.model.PushResponse
import com.projectronin.interop.kafka.spring.KafkaConfig
import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.reflect.KClass

/**
 * Client for managing communication with Kafka.
 */
@Component
class KafkaClient(private val kafkaConfig: KafkaConfig) {
    private val producersByTopicName: ConcurrentSkipListMap<String, RoninProducer> = ConcurrentSkipListMap()
    private val consumersByTopicAndGroup: ConcurrentSkipListMap<String, RoninConsumer> = ConcurrentSkipListMap()

    /**
     * Publishes the [events] to the Kafka [topic] on behalf of a tenant.
     */
    fun <T> publishEvents(
        topic: KafkaTopic,
        events: List<KafkaEvent<T>>,
    ): PushResponse<KafkaEvent<T>> {
        val producer = producersByTopicName.computeIfAbsent(topic.topicName) { createProducer(topic, kafkaConfig) }

        val results =
            events.associateWith { event ->
                val roninEvent =
                    RoninEvent(
                        dataSchema = topic.dataSchema,
                        source = kafkaConfig.publish.source,
                        type = event.type,
                        data = event.data,
                        subject = event.subject,
                        patientId = event.patientId,
                        tenantId = event.tenantId,
                        resourceVersion = event.resourceVersionId,
                    )
                producer.send(roninEvent)
            }.map { (event, future) ->
                runCatching { future.get() }.fold(
                    onSuccess = { Pair(event, null) },
                    onFailure = { Pair(null, Failure(event, it)) },
                )
            }
        return PushResponse(
            successful = results.mapNotNull { it.first },
            failures = results.mapNotNull { it.second },
        )
    }

    fun retrieveEvents(
        topic: KafkaTopic,
        typeMap: Map<String, KClass<*>>,
        groupId: String? = null,
        duration: Duration = Duration.ofMillis(1000),
        limit: Int = 100000,
    ): List<RoninEvent<*>> {
        val consumer =
            consumersByTopicAndGroup.computeIfAbsent("${topic.topicName}|$groupId") {
                createConsumer(
                    topic,
                    typeMap,
                    kafkaConfig,
                    groupId,
                )
            }

        return consume(consumer, duration, limit)
    }

    fun retrieveMultiTopicEvents(
        topicList: List<KafkaTopic>,
        typeMap: Map<String, KClass<*>>,
        groupId: String,
        duration: Duration = Duration.ofMillis(1000),
        limit: Int = 100000,
    ): List<RoninEvent<*>> {
        val consumer =
            consumersByTopicAndGroup.computeIfAbsent("MultiTopicConsumer|$groupId") {
                createMultiConsumer(
                    topicList,
                    typeMap,
                    kafkaConfig,
                    groupId,
                )
            }
        return consume(consumer, duration, limit)
    }

    private fun consume(
        consumer: RoninConsumer,
        duration: Duration,
        limit: Int,
    ): List<RoninEvent<*>> {
        val messageList = mutableListOf<RoninEvent<*>>()
        // initial poll, will return immediately if events exist. Otherwise waits for [duration]
        consumer.pollOnce(duration) {
            messageList.add(it)
            RoninEventResult.ACK
        }
        if (messageList.isNotEmpty()) {
            // keep pulling events in case they are still arriving when the initial poll returns
            var gotMessage = true
            while (gotMessage) {
                gotMessage = false
                if (messageList.size >= limit) break
                consumer.pollOnce(Duration.ofMillis(500)) {
                    gotMessage = true
                    messageList.add(it)
                    RoninEventResult.ACK
                }
            }
        }
        return messageList
    }
}
