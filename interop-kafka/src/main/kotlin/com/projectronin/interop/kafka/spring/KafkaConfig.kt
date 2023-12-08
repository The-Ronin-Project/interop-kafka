package com.projectronin.interop.kafka.spring

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "kafka")
data class KafkaConfig(
    val cloud: KafkaCloudConfig,
    val bootstrap: KafkaBootstrapConfig,
    val publish: KafkaPublishConfig,
    val retrieve: KafkaRetrieveConfig,
    val properties: KafkaPropertiesConfig = KafkaPropertiesConfig(),
)

data class KafkaCloudConfig(
    val vendor: String,
    val region: String,
)

data class KafkaBootstrapConfig(
    val servers: String,
)

data class KafkaPublishConfig(
    val source: String,
)

data class KafkaRetrieveConfig(
    // default group ID
    val groupId: String,
    val serviceId: String = "interop-mirth",
)

data class KafkaPropertiesConfig(
    val security: KafkaSecurityConfig = KafkaSecurityConfig(),
    val sasl: KafkaSaslConfig = KafkaSaslConfig(),
)

data class KafkaSecurityConfig(
    val protocol: String? = null,
)

data class KafkaSaslConfig(
    val mechanism: String? = null,
    val username: String? = null,
    val password: String? = null,
    val jaas: KafkaSaslJaasConfig = KafkaSaslJaasConfig(),
)

data class KafkaSaslJaasConfig(
    val config: String? = null,
)
