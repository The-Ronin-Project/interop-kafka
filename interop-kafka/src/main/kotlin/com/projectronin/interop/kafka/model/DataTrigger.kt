package com.projectronin.interop.kafka.model

/**
 * Enumeration of the types of triggers that could be associated to data loads and publishing.
 */
enum class DataTrigger(val type: String) {
    NIGHTLY("nightly"),

    AD_HOC("adhoc"),

    BACKFILL("backfill")
}
