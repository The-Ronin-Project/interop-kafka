package com.projectronin.interop.kafka.client

import com.projectronin.interop.kafka.model.KafkaTopic
import com.projectronin.interop.kafka.spring.KafkaBootstrapConfig
import com.projectronin.interop.kafka.spring.KafkaCloudConfig
import com.projectronin.interop.kafka.spring.KafkaConfig
import com.projectronin.interop.kafka.spring.KafkaPropertiesConfig
import com.projectronin.interop.kafka.spring.KafkaPublishConfig
import com.projectronin.interop.kafka.spring.KafkaRetrieveConfig
import com.projectronin.interop.kafka.spring.KafkaSaslConfig
import com.projectronin.interop.kafka.spring.KafkaSaslJaasConfig
import com.projectronin.interop.kafka.spring.KafkaSecurityConfig
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class ProducerUtilsTest {
    @Test
    fun `createProducer works`() {
        // This test creates an actual RoninProducer, which means that it also creates a KafkaProducer
        val kafkaConfig = KafkaConfig(
            cloud = KafkaCloudConfig(
                vendor = "local",
                region = "local"
            ),
            bootstrap = KafkaBootstrapConfig(servers = "localhost:9092"),
            publish = KafkaPublishConfig(source = "interop-kafka-it"),
            properties = KafkaPropertiesConfig(
                security = KafkaSecurityConfig(protocol = "PLAINTEXT"),
                sasl = KafkaSaslConfig(
                    mechanism = "GSSAPI",
                    jaas = KafkaSaslJaasConfig(config = "")
                )
            ),
            retrieve = KafkaRetrieveConfig(groupId = "interop-kafka-it")
        )

        val topic = mockk<KafkaTopic> {
            every { dataSchema } returns "test.topic.name.schema"
            every { topicName } returns "topicname"
        }
        val producer = createProducer(topic, kafkaConfig)
        Assertions.assertNotNull(producer)
    }

    @Test
    fun `createConsumer works`() {
        // This test creates an actual RoninProducer, which means that it also creates a KafkaProducer
        val kafkaConfig = KafkaConfig(
            cloud = KafkaCloudConfig(
                vendor = "local",
                region = "local"
            ),
            bootstrap = KafkaBootstrapConfig(servers = "localhost:9092"),
            publish = KafkaPublishConfig(source = "interop-kafka-it"),
            properties = KafkaPropertiesConfig(
                security = KafkaSecurityConfig(protocol = "PLAINTEXT"),
                sasl = KafkaSaslConfig(
                    mechanism = "GSSAPI",
                    jaas = KafkaSaslJaasConfig(config = "")
                )
            ),
            retrieve = KafkaRetrieveConfig(groupId = "interop-kafka-it")
        )

        val topic = mockk<KafkaTopic> {
            every { dataSchema } returns "test.topic.name.schema"
            every { topicName } returns "topicname"
        }
        val consumer = createConsumer(topic, mapOf(), kafkaConfig)
        Assertions.assertNotNull(consumer)
    }
}
