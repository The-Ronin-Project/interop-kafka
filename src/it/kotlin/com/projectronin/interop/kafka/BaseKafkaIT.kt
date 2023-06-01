package com.projectronin.interop.kafka

import com.projectronin.interop.kafka.model.DataTrigger
import com.projectronin.interop.kafka.model.PublishTopic
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
import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File
import java.util.Timer
import kotlin.concurrent.schedule
import kotlin.reflect.KClass

abstract class BaseKafkaIT {
    companion object {
        val docker =
            DockerComposeContainer(File(BaseKafkaIT::class.java.getResource("/docker-compose-kafka.yaml")!!.file)).waitingFor(
                "kafka",
                Wait.forLogMessage(".*\\[KafkaServer id=\\d+\\] started.*", 1)
            ).start()

        private val consumersByTopic = mutableMapOf<String, RoninConsumer>()
    }

    protected val tenantId = "test"
    private val cloudConfig = KafkaCloudConfig(
        vendor = "local",
        region = "local"
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

    protected fun pollEvents(
        topic: PublishTopic,
        trigger: DataTrigger?,
        typeMap: Map<String, KClass<*>>,
        waitTime: Long = 1000,
        expectedResults: Int? = null
    ): List<RoninEvent<*>> {
        val consumer = createConsumer(topic, trigger, typeMap)
        val events = mutableListOf<RoninEvent<*>>()

        Timer("poll").schedule(waitTime) {
            consumer.stop()
        }

        consumer.process {
            events.add(it)

            if (expectedResults != null && events.size >= expectedResults) {
                consumer.stop()
            }

            RoninEventResult.ACK
        }
        return events
    }

    private fun createConsumer(topic: PublishTopic, trigger: DataTrigger?, typeMap: Map<String, KClass<*>>) =
        consumersByTopic.computeIfAbsent(topic.topicName) {
            RoninConsumer(
                topics = listOf(it),
                typeMap = typeMap,
                kafkaProperties = RoninConsumerKafkaProperties(
                    "bootstrap.servers" to kafkaConfig.bootstrap.servers,
                    "group.id" to "interop-kafka-it-$it"
                )
            )
        }
}
