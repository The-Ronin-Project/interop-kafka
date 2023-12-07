package com.projectronin.interop.kafka.model

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class KafkaEventTest {
    @Test
    fun `returns the proper type`() {
        val event =
            KafkaEvent(
                domain = "domain",
                resource = "resource",
                action = KafkaAction.PUBLISH,
                resourceId = "1234",
                data = "Data",
            )
        assertEquals("ronin.domain.resource.publish", event.type)
    }

    @Test
    fun `returns the proper subject`() {
        val event =
            KafkaEvent(
                domain = "domain",
                resource = "resource",
                action = KafkaAction.PUBLISH,
                resourceId = "1234",
                data = "Data",
            )
        assertEquals("ronin.domain.resource/1234", event.subject)
    }
}
