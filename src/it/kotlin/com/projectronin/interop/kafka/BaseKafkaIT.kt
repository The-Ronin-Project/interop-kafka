package com.projectronin.interop.kafka

import com.projectronin.interop.kafka.spring.AdminWrapper
import com.projectronin.interop.kafka.spring.KafkaBootstrapConfig
import com.projectronin.interop.kafka.spring.KafkaCloudConfig
import com.projectronin.interop.kafka.spring.KafkaConfig
import com.projectronin.interop.kafka.spring.KafkaPropertiesConfig
import com.projectronin.interop.kafka.spring.KafkaPublishConfig
import com.projectronin.interop.kafka.spring.KafkaRetrieveConfig
import com.projectronin.interop.kafka.spring.KafkaSaslConfig
import com.projectronin.interop.kafka.spring.KafkaSaslJaasConfig
import com.projectronin.interop.kafka.spring.KafkaSecurityConfig
import mu.KotlinLogging
import org.apache.kafka.common.ConsumerGroupState
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File

abstract class BaseKafkaIT {
    companion object {
        val docker =
            DockerComposeContainer(File(BaseKafkaIT::class.java.getResource("/docker-compose-kafka.yaml")!!.file)).waitingFor(
                "kafka",
                Wait.forLogMessage(".*\\[KafkaServer id=\\d+\\] started.*", 1)
            ).start()
    }

    private val logger = KotlinLogging.logger { }

    protected val tenantId = "test"
    private val cloudConfig = KafkaCloudConfig(
        vendor = "oci",
        region = "us-phoenix-1"
    )

    protected val kafkaConfig = KafkaConfig(
        cloud = cloudConfig,
        bootstrap = KafkaBootstrapConfig(servers = "localhost:9092"),
        publish = KafkaPublishConfig(source = "interop-kafka-it"),
        properties = KafkaPropertiesConfig(
            security = KafkaSecurityConfig(protocol = "PLAINTEXT"),
            sasl = KafkaSaslConfig(
                mechanism = "GSSAPI",
                jaas = KafkaSaslJaasConfig(config = "")
            )
        ),
        retrieve = KafkaRetrieveConfig("groupID")
    )

    protected val kafkaAdmin = AdminWrapper(kafkaConfig)

    protected fun waitOnTopic(topic: String) {
        // Wait for the topic to be registered and the consumer group to be stable
        var count = 0
        while (count < 100) {
            val names = kafkaAdmin.client.listTopics().names().get()
            if (names.any { it == topic }) {
                val group = kafkaAdmin.client.listConsumerGroups().valid().get().find { it.groupId() == "groupID" }
                if (group != null) {
                    // Only attempt 1200 times, or 2 minutes
                    repeat(1200) {
                        val state = group.state().get()
                        if (state == ConsumerGroupState.STABLE) {
                            logger.warn { "Topic and consumer group created" }
                            return
                        }

                        logger.warn { "Consumer group state: $state" }
                        Thread.sleep(100)
                    }
                } else {
                    logger.warn { "Topic found, but consumer group not found" }
                }
            } else {
                logger.warn { "Topic not yet found" }
            }
            Thread.sleep(100)
            count += 1
        }
    }
}
