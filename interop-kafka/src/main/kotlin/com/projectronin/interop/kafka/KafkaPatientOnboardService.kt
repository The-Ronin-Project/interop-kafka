package com.projectronin.interop.kafka

import com.projectronin.interop.kafka.client.KafkaClient
import com.projectronin.interop.kafka.model.ExternalTopic
import org.springframework.stereotype.Service

/**
 * Service responsible for grabbing onboard events from Kafka
 */
@Service
class KafkaPatientOnboardService(private val kafkaClient: KafkaClient) {

    private val onboardTopic = ExternalTopic(
        systemName = "chokuto",
        topicName = "oci.us-phoenix-1.chokuto.patient-onboarding-status-publish.v1",
        dataSchema = "https://github.com/projectronin/contract-event-prodeng-patient-onboarding-status/blob/main/v1/patient-onboarding-status.schema.json"
    )

    /**
     * Grabs onboarding-style events from Kafka.
     */
    fun retrieveOnboardEvents(
        groupId: String? = null
    ): List<PatientOnboardingStatus> {
        val typeMap = mapOf("ronin.patient.onboarding.create" to PatientOnboardingStatus::class)
        val events = kafkaClient.retrieveEvents(onboardTopic, typeMap, groupId)
        return events.map {
            it.data as PatientOnboardingStatus
        }
    }
}

// need to create this manually until mirth is upgraded to Java 17
data class PatientOnboardingStatus(
    val patientId: String,
    val tenantId: String,
    val action: OnboardAction,
    val actionTimestamp: String
) {
    enum class OnboardAction {
        ONBOARD, OFFBOARD
    }
}
