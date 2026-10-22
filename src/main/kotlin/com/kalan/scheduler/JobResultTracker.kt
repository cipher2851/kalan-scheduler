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
    private val resultTypeCountsByJob = ConcurrentHashMap<String, ConcurrentHashMap<Class<out JobResult>, AtomicInteger>>()

    override fun onResult(jobId: String, result: JobResult) {
        lastResults[jobId] = result
        val resultType = result::class.java
        resultTypeCountsByJob
            .computeIfAbsent(jobId) { ConcurrentHashMap() }
            .computeIfAbsent(resultType) { AtomicInteger(0) }
            .incrementAndGet()
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

    fun getSuccessRate(jobId: String): Double {
        val successes = getSuccessCount(jobId)
        val failures = getFailureCount(jobId)
        val total = successes + failures
        return if (total == 0) 0.0 else successes.toDouble() / total
    }

    fun getTotalSuccessCount(): Int = successCounts.values.sumOf { it.get() }

    fun getTotalFailureCount(): Int = failureCounts.values.sumOf { it.get() }

    fun getTotalExecutionCount(): Int = getTotalSuccessCount() + getTotalFailureCount()

    fun getOverallFailureRate(): Double {
        val total = getTotalExecutionCount()
        return if (total == 0) 0.0 else getTotalFailureCount().toDouble() / total
    }

    fun getTrackedJobIds(): Set<String> =
        (successCounts.keys + failureCounts.keys + lastResults.keys + resultTypeCountsByJob.keys).toSet()

    fun getUnhealthyJobs(failureThreshold: Int = 3): List<String> {
        return getTrackedJobIds().filter { getFailureCount(it) >= failureThreshold }
    }

    fun clear(jobId: String) {
        successCounts.remove(jobId)
        failureCounts.remove(jobId)
        lastResults.remove(jobId)
        resultTypeCountsByJob.remove(jobId)
    }

    fun clearAll() {
        successCounts.clear()
        failureCounts.clear()
        lastResults.clear()
        resultTypeCountsByJob.clear()
    }

    fun getSummary(jobId: String): Map<String, Any> {
        return mapOf(
            "jobId" to jobId,
            "successes" to getSuccessCount(jobId),
            "failures" to getFailureCount(jobId),
            "lastResult" to (getLastResult(jobId) ?: "NONE")
        )
    }

    /**
     * Returns the number of recorded results whose runtime type is [resultClass].
     */
    fun getResultTypeCount(resultClass: Class<out JobResult>): Int =
        resultTypeCountsByJob.values.sumOf { counts -> counts[resultClass]?.get() ?: 0 }

    /**
     * Returns an immutable snapshot of recorded result counts keyed by result type.
     */
    fun getResultTypeCounts(): Map<Class<out JobResult>, Int> {
        val aggregated = mutableMapOf<Class<out JobResult>, Int>()
        resultTypeCountsByJob.values.forEach { counts ->
            counts.forEach { (resultType, count) ->
                aggregated[resultType] = (aggregated[resultType] ?: 0) + count.get()
            }
        }
        return aggregated
    }

    /**
     * Returns the most frequently recorded result type, or null when no results
     * have been recorded yet.
     */
    fun getMostCommonResultType(): Class<out JobResult>? =
        getResultTypeCounts().maxByOrNull { it.value }?.key
}
