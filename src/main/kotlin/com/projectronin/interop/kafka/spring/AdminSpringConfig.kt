package com.projectronin.interop.kafka.spring

import com.projectronin.interop.kafka.client.createProducerProperties
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AdminSpringConfig {

    @Bean
    fun kafkaAdmin(@Autowired kafkaConfig: KafkaConfig): AdminWrapper {
        return AdminWrapper(kafkaConfig)
    }
}

class AdminWrapper(kafkaConfig: KafkaConfig) {
    val client: AdminClient by lazy { KafkaAdminClient.create(createProducerProperties(kafkaConfig).properties) }
}
