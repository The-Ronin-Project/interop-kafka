package com.projectronin.interop.kafka.spring

import com.projectronin.interop.kafka.model.RequestTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class RequestSpringConfig(private val kafkaSpringConfig: KafkaConfig) {
    @Bean
    fun requestTopic(): RequestTopic {
        val topicParameters =
            listOf(
                kafkaSpringConfig.cloud.vendor,
                kafkaSpringConfig.cloud.region,
                "interop-mirth",
                "resource-request",
                "v1",
            )
        @Suppress("ktlint:standard:max-line-length")
        return RequestTopic(
            systemName = "interop-mirth",
            topicName = topicParameters.joinToString("."),
            dataSchema = "https://github.com/projectronin/contract-event-interop-resource-request/blob/main/v1/interop-resource-request-v1.schema.json",
        )
    }
}
