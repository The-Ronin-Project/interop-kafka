package com.projectronin.interop.kafka

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
}
