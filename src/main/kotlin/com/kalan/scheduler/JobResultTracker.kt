package com.kalan.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * A JobResultListener implementation that aggregates the outcomes of multiple jobs.
 * This is useful for monitoring the overall health of a job group or a batch.
 */
class JobResultTracker : JobResultListener {
    private val successCounts = ConcurrentHashMap<String, AtomicInteger>()
    private val failureCounts = ConcurrentHashMap<String, AtomicInteger>()
    private val lastResults = ConcurrentHashMap<String, JobResult>()

    override fun onResult(jobId: String, result: JobResult) {
        lastResults[jobId] = result
        if (result is JobResult.Success) {
            successCounts.computeIfAbsent(jobId) { AtomicInteger(0) }.incrementAndGet()
        } else if (result is JobResult.Failure) {
            failureCounts.computeIfAbsent(jobId) { AtomicInteger(0) }.incrementAndGet()
        }
    }

    fun getSuccessCount(jobId: String): Int = successCounts[jobId]?.get() ?: 0
    
    fun getFailureCount(jobId: String): Int = failureCounts[jobId]?.get() ?: 0

    fun getLastResult(jobId: String): JobResult? = lastResults[jobId]

    fun isJobHealthy(jobId: String, failureThreshold: Int = 3): Boolean {
        return getFailureCount(jobId) < failureThreshold
    }

    fun clear(jobId: String) {
        successCounts.remove(jobId)
        failureCounts.remove(jobId)
        lastResults.remove(jobId)
    }

    fun clearAll() {
        successCounts.clear()
        failureCounts.clear()
        lastResults.clear()
    }

    fun getSummary(jobId: String): Map<String, Any> {
        return mapOf(
            "jobId" to jobId,
            "successes" to getSuccessCount(jobId),
            "failures" to getFailureCount(jobId),
            "lastResult" to (getLastResult(jobId) ?: "NONE")
        )
    }
}
