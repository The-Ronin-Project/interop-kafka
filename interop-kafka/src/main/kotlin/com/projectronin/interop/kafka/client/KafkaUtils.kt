package com.projectronin.interop.kafka.client

import com.projectronin.interop.kafka.model.KafkaTopic
import com.projectronin.interop.kafka.spring.KafkaConfig
import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.config.RoninProducerKafkaProperties
import kotlin.reflect.KClass

/**
 * Creates a [RoninProducer] capable of publishing to the [KafkaTopic] represented by [topic].
 */
fun createProducer(
    topic: KafkaTopic,
    kafkaConfig: KafkaConfig
): RoninProducer {
    return RoninProducer(
        topic = topic.topicName,
        source = kafkaConfig.publish.source,
        dataSchema = topic.dataSchema,
        kafkaProperties = createProducerProperties(kafkaConfig)
    )
}

fun createProducerProperties(kafkaConfig: KafkaConfig): RoninProducerKafkaProperties {
    val kafkaProperties = kafkaConfig.properties
    return RoninProducerKafkaProperties(
        "bootstrap.servers" to kafkaConfig.bootstrap.servers,
        "security.protocol" to kafkaProperties.security.protocol,
        "sasl.mechanism" to kafkaProperties.sasl.mechanism,
        "sasl.jaas.config" to kafkaProperties.sasl.jaas.config
    )
}

/**
 * Creates a [RoninConsumer] capable of consuming the [KafkaTopic] represented by [topic],
 * allows for consumer group to be overridden with [overriddenGroupId].
 */
fun createConsumer(
    topic: KafkaTopic,
    typeMap: Map<String, KClass<*>>,
    kafkaConfig: KafkaConfig,
    overriddenGroupId: String? = null
): RoninConsumer {
    val kafkaProperties = kafkaConfig.properties
    // allow consumers to initialize from the current offset rather than the very first
    // used by mirth channels as we add to the DAG
    val offset = if (topic.useLatestOffset) { "latest" } else { "earliest" }
    val consumerProperties = RoninConsumerKafkaProperties(
        "bootstrap.servers" to kafkaConfig.bootstrap.servers,
        "security.protocol" to kafkaProperties.security.protocol,
        "sasl.mechanism" to kafkaProperties.sasl.mechanism,
        "sasl.jaas.config" to kafkaProperties.sasl.jaas.config,
        "group.id" to (overriddenGroupId ?: kafkaConfig.retrieve.groupId),
        "auto.offset.reset" to offset
    )
    return RoninConsumer(
        topics = listOf(topic.topicName),
        typeMap = typeMap,
        kafkaProperties = consumerProperties
    )
}

/**
 * Creates a [RoninConsumer] capable of consuming the [KafkaTopic]s represented by [topics],
 * allows for consumer group to be overridden with [overriddenGroupId].
 */
fun createMultiConsumer(
    topics: List<KafkaTopic>,
    typeMap: Map<String, KClass<*>>,
    kafkaConfig: KafkaConfig,
    overriddenGroupId: String? = null
): RoninConsumer {
    val kafkaProperties = kafkaConfig.properties
    // allow consumers to initialize from the current offset rather than the very first
    // used by mirth channels as we add to the DAG
    val consumerProperties = RoninConsumerKafkaProperties(
        "bootstrap.servers" to kafkaConfig.bootstrap.servers,
        "security.protocol" to kafkaProperties.security.protocol,
        "sasl.mechanism" to kafkaProperties.sasl.mechanism,
        "sasl.jaas.config" to kafkaProperties.sasl.jaas.config,
        "group.id" to (overriddenGroupId ?: kafkaConfig.retrieve.groupId),
        "auto.offset.reset" to "latest"
    )
    return RoninConsumer(
        topics = topics.map { it.topicName },
        typeMap = typeMap,
        kafkaProperties = consumerProperties
    )
}
