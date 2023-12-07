package com.projectronin.interop.kafka.model

import com.projectronin.interop.fhir.r4.resource.Resource

data class PublishResourceWrapper(
    val resource: Resource<*>,
    val embeddedResources: List<Resource<*>> = emptyList(),
) {
    val resourceType = resource.resourceType
    val id = resource.id
}
