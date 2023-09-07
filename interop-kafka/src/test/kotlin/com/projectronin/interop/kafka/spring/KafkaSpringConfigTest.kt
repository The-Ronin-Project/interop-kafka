package com.projectronin.interop.kafka.spring

import com.projectronin.interop.kafka.KafkaPublishService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.getBean
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import

class KafkaSpringConfigTest {
    private val minimumConfigMap = mapOf(
        "kafka.cloud.vendor" to "oci",
        "kafka.cloud.region" to "us-phoenix-1",
        "kafka.bootstrap.servers" to "localhost:9092",
        "kafka.publish.source" to "interop-kafka-test",
        "kafka.retrieve.groupId" to "interop-kafka-test",
        "kafka.properties.sasl.jaas.config" to "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"\${kafka.sasl.username}\" password=\"\${kafka.sasl.password}\";",
        "kafka.properties.sasl.mechanism" to "SCRAM-SHA-512",
        "kafka.properties.sasl.username" to "saslUsername",
        "kafka.properties.sasl.password" to "saslPassword",
        "kafka.properties.security.protocol" to "SASL_SSL"
    )

    @Test
    fun `can build KafkaConfig with minimum required data`() {
        val config = runApplication(minimumConfigMap)

        assertEquals(
            KafkaConfig(
                cloud = KafkaCloudConfig(
                    vendor = "oci",
                    region = "us-phoenix-1"
                ),
                bootstrap = KafkaBootstrapConfig(
                    servers = "localhost:9092"
                ),
                publish = KafkaPublishConfig(
                    source = "interop-kafka-test"
                ),
                retrieve = KafkaRetrieveConfig(
                    groupId = "interop-kafka-test"
                ),
                properties = KafkaPropertiesConfig(
                    security = KafkaSecurityConfig(
                        protocol = "SASL_SSL"
                    ),
                    sasl = KafkaSaslConfig(
                        username = "saslUsername",
                        password = "saslPassword",
                        mechanism = "SCRAM-SHA-512",
                        jaas = KafkaSaslJaasConfig(
                            config = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"\${kafka.sasl.username}\" password=\"\${kafka.sasl.password}\";"
                        )
                    )
                )
            ),
            config
        )
    }

    @Test
    fun `loads KafkaPublishService`() {
        runApplication(minimumConfigMap)

        val service = TestApplication.context!!.getBean<KafkaPublishService>()
        assertNotNull(service)
        assertInstanceOf(KafkaPublishService::class.java, service)
    }

    private fun runApplication(properties: Map<String, String>): KafkaConfig? {
        val application = SpringApplication(TestApplication::class.java)
        application.setDefaultProperties(properties)
        application.run()

        return TestApplication.received
    }

    // This application will cause failures if KafkaConfig can not be properly created.
    @SpringBootApplication
    @Import(KafkaSpringConfig::class)
    class TestApplication(private val kafkaConfig: KafkaConfig) {
        companion object {
            var context: ApplicationContext? = null
            var received: KafkaConfig? = null
        }

        @Bean
        fun commandLineRunner(ctx: ApplicationContext) = CommandLineRunner { args ->
            context = ctx
            received = kafkaConfig
        }
    }
}
