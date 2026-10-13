package com.kalan.scheduler

/**
 * A predicate for filtering jobs based on their configuration or runtime state.
 */
fun interface JobFilter {
    fun test(job: Job): Boolean

    companion object {
        fun byTag(tag: String) = JobFilter { it.tags.contains(tag) }
        fun byPriority(priority: JobPriority) = JobFilter { it.priority == priority }
        fun byStatus(status: JobExecutionStatus) = JobFilter { it.getJobStatus() == status }
        fun byMetadata(key: String, value: Any) = JobFilter { it.metadata[key] == value }
        fun anyOf(vararg filters: JobFilter) = JobFilter { job -> filters.any { it.test(job) } }
        fun allOf(vararg filters: JobFilter) = JobFilter { job -> filters.all { it.test(job) } }
        fun not(filter: JobFilter) = JobFilter { job -> !filter.test(job) }
    }
}

/**
 * Utility for applying a filter to a collection of jobs.
 */
fun Collection<Job>.filterJobs(filter: JobFilter): List<Job> {
    return this.filter { filter.test(it) }
}