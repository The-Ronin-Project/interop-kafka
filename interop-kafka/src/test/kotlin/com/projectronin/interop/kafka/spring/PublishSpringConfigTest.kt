package com.projectronin.interop.kafka.spring

import com.projectronin.event.interop.internal.v1.InteropResourcePublishV1
import com.projectronin.event.interop.internal.v1.Metadata
import com.projectronin.event.interop.internal.v1.ResourceType
import com.projectronin.interop.fhir.r4.datatype.primitive.Id
import com.projectronin.interop.fhir.r4.resource.Organization
import com.projectronin.interop.fhir.r4.resource.Patient
import com.projectronin.interop.fhir.r4.resource.Practitioner
import com.projectronin.interop.kafka.model.DataTrigger
import com.projectronin.interop.kafka.model.PublishResourceWrapper
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PublishSpringConfigTest {
    private val kafkaConfig =
        mockk<KafkaConfig> {
            every { retrieve.serviceId } returns "serviceId"
            every { cloud } returns
                mockk {
                    every { vendor } returns "vendor"
                    every { region } returns "region"
                }
        }
    private val patientTopics = PublishSpringConfig(kafkaConfig).generateTopics(ResourceType.Patient)

    @Test
    fun `nightly topic creates proper event for resource with no embedded resources`() {
        val nightlyTopic = patientTopics.first { it.dataTrigger == DataTrigger.NIGHTLY }

        val patient =
            Patient(
                id = Id("123"),
            )
        val resourceWrapper = PublishResourceWrapper(patient)
        val metadata = mockk<Metadata>()

        val event = nightlyTopic.converter.invoke("tenant", resourceWrapper, metadata) as InteropResourcePublishV1
        assertEquals("tenant", event.tenantId)
        assertEquals("""{"resourceType":"Patient","id":"123"}""", event.resourceJson)
        assertEquals(ResourceType.Patient, event.resourceType)
        assertEquals(InteropResourcePublishV1.DataTrigger.nightly, event.dataTrigger)
        assertEquals(metadata, event.metadata)

        val embeddedResources = event.embeddedResources ?: emptyList()
        assertEquals(0, embeddedResources.size)
    }

    @Test
    fun `nightly topic creates proper event for resource with embedded resources`() {
        val nightlyTopic = patientTopics.first { it.dataTrigger == DataTrigger.NIGHTLY }

        val patient =
            Patient(
                id = Id("123"),
            )
        val practitioner =
            Practitioner(
                id = Id("456"),
            )
        val organization =
            Organization(
                id = Id("789"),
            )
        val resourceWrapper = PublishResourceWrapper(patient, listOf(practitioner, organization))
        val metadata = mockk<Metadata>()

        val event = nightlyTopic.converter.invoke("tenant", resourceWrapper, metadata) as InteropResourcePublishV1
        assertEquals("tenant", event.tenantId)
        assertEquals("""{"resourceType":"Patient","id":"123"}""", event.resourceJson)
        assertEquals(ResourceType.Patient, event.resourceType)
        assertEquals(InteropResourcePublishV1.DataTrigger.nightly, event.dataTrigger)
        assertEquals(metadata, event.metadata)

        val embeddedResources = event.embeddedResources ?: emptyList()
        assertEquals(2, embeddedResources.size)

        assertEquals(ResourceType.Practitioner, embeddedResources[0].resourceType)
        assertEquals("""{"resourceType":"Practitioner","id":"456"}""", embeddedResources[0].resourceJson)

        assertEquals(ResourceType.Organization, embeddedResources[1].resourceType)
        assertEquals("""{"resourceType":"Organization","id":"789"}""", embeddedResources[1].resourceJson)
    }

    @Test
    fun `adhoc topic creates proper event for resource with no embedded resources`() {
        val nightlyTopic = patientTopics.first { it.dataTrigger == DataTrigger.AD_HOC }

        val patient =
            Patient(
                id = Id("123"),
            )
        val resourceWrapper = PublishResourceWrapper(patient)
        val metadata = mockk<Metadata>()

        val event = nightlyTopic.converter.invoke("tenant", resourceWrapper, metadata) as InteropResourcePublishV1
        assertEquals("tenant", event.tenantId)
        assertEquals("""{"resourceType":"Patient","id":"123"}""", event.resourceJson)
        assertEquals(ResourceType.Patient, event.resourceType)
        assertEquals(InteropResourcePublishV1.DataTrigger.adhoc, event.dataTrigger)
        assertEquals(metadata, event.metadata)

        val embeddedResources = event.embeddedResources ?: emptyList()
        assertEquals(0, embeddedResources.size)
    }

    @Test
    fun `adhoc topic creates proper event for resource with embedded resources`() {
        val nightlyTopic = patientTopics.first { it.dataTrigger == DataTrigger.AD_HOC }

        val patient =
            Patient(
                id = Id("123"),
            )
        val practitioner =
            Practitioner(
                id = Id("456"),
            )
        val organization =
            Organization(
                id = Id("789"),
            )
        val resourceWrapper = PublishResourceWrapper(patient, listOf(practitioner, organization))
        val metadata = mockk<Metadata>()

        val event = nightlyTopic.converter.invoke("tenant", resourceWrapper, metadata) as InteropResourcePublishV1
        assertEquals("tenant", event.tenantId)
        assertEquals("""{"resourceType":"Patient","id":"123"}""", event.resourceJson)
        assertEquals(ResourceType.Patient, event.resourceType)
        assertEquals(InteropResourcePublishV1.DataTrigger.adhoc, event.dataTrigger)
        assertEquals(metadata, event.metadata)

        val embeddedResources = event.embeddedResources ?: emptyList()
        assertEquals(2, embeddedResources.size)

        assertEquals(ResourceType.Practitioner, embeddedResources[0].resourceType)
        assertEquals("""{"resourceType":"Practitioner","id":"456"}""", embeddedResources[0].resourceJson)

        assertEquals(ResourceType.Organization, embeddedResources[1].resourceType)
        assertEquals("""{"resourceType":"Organization","id":"789"}""", embeddedResources[1].resourceJson)
    }

    @Test
    fun `backfill topic creates proper event for resource with  no embedded resources`() {
        val backfillTopic = patientTopics.first { it.dataTrigger == DataTrigger.BACKFILL }

        val patient =
            Patient(
                id = Id("123"),
            )
        val resourceWrapper = PublishResourceWrapper(patient)
        val metadata = mockk<Metadata>()

        val event = backfillTopic.converter.invoke("tenant", resourceWrapper, metadata) as InteropResourcePublishV1
        assertEquals("tenant", event.tenantId)
        assertEquals("""{"resourceType":"Patient","id":"123"}""", event.resourceJson)
        assertEquals(ResourceType.Patient, event.resourceType)
        assertEquals(InteropResourcePublishV1.DataTrigger.backfill, event.dataTrigger)
        assertEquals(metadata, event.metadata)

        val embeddedResources = event.embeddedResources ?: emptyList()
        assertEquals(0, embeddedResources.size)
    }

    @Test
    fun `backfill topic creates proper event for resource with embedded resources`() {
        val backfillTopic = patientTopics.first { it.dataTrigger == DataTrigger.BACKFILL }

        val patient =
            Patient(
                id = Id("123"),
            )
        val practitioner =
            Practitioner(
                id = Id("456"),
            )
        val organization =
            Organization(
                id = Id("789"),
            )
        val resourceWrapper = PublishResourceWrapper(patient, listOf(practitioner, organization))
        val metadata = mockk<Metadata>()

        val event = backfillTopic.converter.invoke("tenant", resourceWrapper, metadata) as InteropResourcePublishV1
        assertEquals("tenant", event.tenantId)
        assertEquals("""{"resourceType":"Patient","id":"123"}""", event.resourceJson)
        assertEquals(ResourceType.Patient, event.resourceType)
        assertEquals(InteropResourcePublishV1.DataTrigger.backfill, event.dataTrigger)
        assertEquals(metadata, event.metadata)

        val embeddedResources = event.embeddedResources ?: emptyList()
        assertEquals(2, embeddedResources.size)

        assertEquals(ResourceType.Practitioner, embeddedResources[0].resourceType)
        assertEquals("""{"resourceType":"Practitioner","id":"456"}""", embeddedResources[0].resourceJson)

        assertEquals(ResourceType.Organization, embeddedResources[1].resourceType)
        assertEquals("""{"resourceType":"Organization","id":"789"}""", embeddedResources[1].resourceJson)
    }
}
