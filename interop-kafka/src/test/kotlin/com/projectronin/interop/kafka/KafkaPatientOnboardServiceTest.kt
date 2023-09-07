package com.projectronin.interop.kafka

import com.projectronin.interop.kafka.client.KafkaClient
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class KafkaPatientOnboardServiceTest {
    private val kafkaClient: KafkaClient = mockk()
    private val service = KafkaPatientOnboardService(kafkaClient)

    @Test
    fun `retrieve events happy path`() {
        val onboardStatus =
            PatientOnboardingStatus("12345", "tenant", PatientOnboardingStatus.OnboardAction.ONBOARD, "timestamp")
        every { kafkaClient.retrieveEvents(any(), any(), any()) } returns listOf(
            mockk {
                every { data as PatientOnboardingStatus } returns onboardStatus
            }
        )
        val ret = service.retrieveOnboardEvents()
        assertEquals(ret.first().patientId, "12345")
    }
}
