package com.projectronin.interop.kafka.model

/**
 * A KafkaEvent contains the raw data necessary to publish data to a Kafka topic.
 */
data class KafkaEvent<T>(
    private val domain: String,
    private val resource: String,
    private val action: KafkaAction,
    private val resourceId: String,
    val data: T,
    val tenantId: String? = null,
    val patientId: String? = null,
    val resourceVersionId: Int? = null,
) {
    private val base = "ronin.$domain.$resource"
    val type = "$base.${action.type}"
    val subject = "$base/$resourceId"
}
