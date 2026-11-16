package com.kalan.scheduler

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger

/**
 * A global audit log that tracks executions across all jobs in the scheduler.
 * This provides a system-wide timeline of what ran, when, and with what outcome.
 */
class JobExecutionAuditLog(private val maxLogSize: Int = 1000) {
    private val logs = ConcurrentLinkedDeque<AuditEntry>()
    private val totalProcessed = AtomicInteger(0)

    data class AuditEntry(
        val timestamp: Instant,
        val jobId: String,
        val executionId: String,
        val result: JobResult,
        val durationMs: Long
    )

    fun record(jobId: String, executionId: String, result: JobResult, durationMs: Long) {
        val entry = AuditEntry(
            timestamp = Instant.now(),
            jobId = jobId,
            executionId = executionId,
            result = result,
            durationMs = durationMs
        )
        
        logs.addFirst(entry)
        totalProcessed.incrementAndGet()

        // Maintain a sliding window of the last N executions
        while (logs.size > maxLogSize) {
            logs.removeLast()
        }
    }

    fun getRecentEntries(limit: Int = 100): List<AuditEntry> {
        return logs.take(limit)
    }

    fun getEntriesSince(since: Instant): List<AuditEntry> {
        return logs.filter { it.timestamp.isAfter(since) }
    }

    fun getEntriesForJob(jobId: String): List<AuditEntry> {
        return logs.filter { it.jobId == jobId }
    }

    fun getEntriesByResultType(resultClass: Class<out JobResult>): List<AuditEntry> {
        return logs.filter { resultClass.isInstance(it.result) }
    }

    fun getEntriesByResultTypes(resultClasses: Set<Class<out JobResult>>): List<AuditEntry> {
        return logs.filter { entry -> resultClasses.any { it.isInstance(entry.result) } }
    }

    fun getEntriesForJobWithResultType(jobId: String, resultClass: Class<out JobResult>): List<AuditEntry> {
        return logs.filter { it.jobId == jobId && resultClass.isInstance(it.result) }
    }

    fun getSuccessCount(): Int = logs.count { it.result is JobResult.Success }

    fun getFailureCount(): Int = logs.count { it.result is JobResult.Failure }

    fun getSuccessCount(jobId: String): Int =
        getEntriesForJob(jobId).count { it.result is JobResult.Success }

    fun getFailureCount(jobId: String): Int =
        getEntriesForJob(jobId).count { it.result is JobResult.Failure }

    fun getResultTypeCount(resultClass: Class<out JobResult>): Int =
        logs.count { resultClass.isInstance(it.result) }

    fun getResultTypeCount(jobId: String, resultClass: Class<out JobResult>): Int =
        getEntriesForJob(jobId).count { resultClass.isInstance(it.result) }

    fun getResultTypeCounts(): Map<Class<out JobResult>, Int> {
        val aggregated = mutableMapOf<Class<out JobResult>, Int>()
        logs.forEach { entry ->
            val resultType = entry.result::class.java
            aggregated[resultType] = (aggregated[resultType] ?: 0) + 1
        }
        val sorted = java.util.TreeMap<Class<out JobResult>, Int>(compareBy { it.name })
        sorted.putAll(aggregated)
        return java.util.Collections.unmodifiableMap(sorted)
    }

    fun getResultTypeCounts(jobId: String): Map<Class<out JobResult>, Int> {
        val aggregated = mutableMapOf<Class<out JobResult>, Int>()
        getEntriesForJob(jobId).forEach { entry ->
            val resultType = entry.result::class.java
            aggregated[resultType] = (aggregated[resultType] ?: 0) + 1
        }
        val sorted = java.util.TreeMap<Class<out JobResult>, Int>(compareBy { it.name })
        sorted.putAll(aggregated)
        return java.util.Collections.unmodifiableMap(sorted)
    }

    fun getMostCommonResultType(): Class<out JobResult>? =
        getResultTypeCounts().entries
            .sortedWith(compareByDescending { it.value }.thenBy { it.key.name })
            .firstOrNull()
            ?.key

    fun getMostCommonResultType(jobId: String): Class<out JobResult>? =
        getResultTypeCounts(jobId).entries
            .sortedWith(compareByDescending { it.value }.thenBy { it.key.name })
            .firstOrNull()
            ?.key

    fun getTotalExecutionTimeMs(): Long {
        return logs.sumOf { it.durationMs }
    }

    fun getTotalExecutionTimeSince(since: Instant): Long {
        return logs.filter { it.timestamp.isAfter(since) }.sumOf { it.durationMs }
    }

    fun getTotalExecutionTimeForResult(resultClass: Class<out JobResult>): Long {
        return logs.filter { resultClass.isInstance(it.result) }.sumOf { it.durationMs }
    }

    fun getTotalProcessedCount(): Int = totalProcessed.get()

    fun getSuccessRate(jobId: String): Double {
        val entries = getEntriesForJob(jobId)
        val total = entries.size
        val successes = entries.count { it.result is JobResult.Success }
        return if (total == 0) 0.0 else successes.toDouble() / total
    }

    fun getFailureRate(jobId: String): Double {
        val entries = getEntriesForJob(jobId)
        val total = entries.size
        val failures = entries.count { it.result is JobResult.Failure }
        return if (total == 0) 0.0 else failures.toDouble() / total
    }

    fun getOverallSuccessRate(): Double {
        val entries = logs.toList()
        val total = entries.size
        val successes = entries.count { it.result is JobResult.Success }
        return if (total == 0) 0.0 else successes.toDouble() / total
    }

    fun getOverallFailureRate(): Double {
        val entries = logs.toList()
        val total = entries.size
        val failures = entries.count { it.result is JobResult.Failure }
        return if (total == 0) 0.0 else failures.toDouble() / total
    }

    fun getSuccessRate(): Double = getOverallSuccessRate()

    fun getFailureRate(): Double = getOverallFailureRate()

    /**
     * Provides a summary of audit records for a specific job within the current log window.
     */
    fun getJobAuditSummary(jobId: String): JobAuditSummary {
        val jobEntries = getEntriesForJob(jobId)
        val total = jobEntries.size
        val successes = jobEntries.count { it.result is JobResult.Success }
        val failures = jobEntries.count { it.result is JobResult.Failure }
        val avgDuration = if (total > 0) jobEntries.map { it.durationMs }.average() else 0.0
        val resultTypeCounts = jobEntries.groupingBy { it.result::class.java }.eachCount()
        val mostCommonResultType = resultTypeCounts.entries
            .sortedWith(compareByDescending { it.value }.thenBy { it.key.name })
            .firstOrNull()
            ?.key

        return JobAuditSummary(
            jobId = jobId,
            totalExecutionsInWindow = total,
            successCount = successes,
            failureCount = failures,
            averageDurationMs = avgDuration,
            successRate = if (total > 0) successes.toDouble() / total else 0.0,
            failureRate = if (total > 0) failures.toDouble() / total else 0.0,
            mostCommonResultType = mostCommonResultType,
            resultTypeCounts = getResultTypeCounts(jobId)
        )
    }

    fun getOverallAuditSummary(): Map<String, Any> {
        val entries = logs.toList()
        val total = entries.size
        val successes = entries.count { it.result is JobResult.Success }
        val failures = entries.count { it.result is JobResult.Failure }
        val totalDurationMs = entries.sumOf { it.durationMs }
        val averageDurationMs = if (total > 0) entries.map { it.durationMs }.average() else 0.0
        val uniqueJobs = entries.map { it.jobId }.distinct().size
        val mostCommonResultType = entries.groupingBy { it.result::class.java }
            .eachCount()
            .entries
            .sortedWith(compareByDescending { it.value }.thenBy { it.key.name })
            .firstOrNull()
            ?.key

        return mapOf(
            "totalExecutionsInWindow" to total,
            "totalProcessed" to totalProcessed.get(),
            "uniqueJobs" to uniqueJobs,
            "successes" to successes,
            "failures" to failures,
            "successRate" to getOverallSuccessRate(),
            "failureRate" to getOverallFailureRate(),
            "totalDurationMs" to totalDurationMs,
            "averageDurationMs" to averageDurationMs,
            "mostCommonResultType" to (mostCommonResultType?.name ?: "NONE"),
            "resultTypeCounts" to getResultTypeCounts()
        )
    }

    fun getOverallSummary(): Map<String, Any> = getOverallAuditSummary()

    /**
     * Returns a health-oriented summary for a single job based on its recorded
     * audit entries. Jobs with no recorded entries are considered healthy.
     */
    fun getJobHealthSummary(jobId: String, failureThreshold: Double = 0.5): Map<String, Any> {
        val entries = getEntriesForJob(jobId)
        val total = entries.size
        val failures = entries.count { it.result is JobResult.Failure }
        val failureRate = if (total == 0) 0.0 else failures.toDouble() / total
        val healthy = total == 0 || failureRate <= failureThreshold
        return mapOf(
            "jobId" to jobId,
            "totalExecutionsInWindow" to total,
            "failures" to failures,
            "failureRate" to failureRate,
            "healthy" to healthy,
            "isHealthy" to healthy,
            "healthStatus" to if (healthy) "HEALTHY" else "UNHEALTHY",
            "failureThreshold" to failureThreshold,
            "mostCommonResultType" to (getMostCommonResultType(jobId)?.name ?: "NONE"),
            "resultTypeCounts" to getResultTypeCounts(jobId)
        )
    }

    /**
     * Returns an aggregate health summary across all jobs represented in the
     * audit log. A job is healthy when its recorded failure rate does not exceed
     * [failureThreshold].
     */
    fun getOverallHealthSummary(failureThreshold: Double = 0.5): Map<String, Any> {
        val entries = logs.toList()
        val jobIds = entries.map { it.jobId }.distinct()
        val healthyJobs = jobIds.count { jobId ->
            val jobEntries = getEntriesForJob(jobId)
            val total = jobEntries.size
            val failures = jobEntries.count { it.result is JobResult.Failure }
            val failureRate = if (total == 0) 0.0 else failures.toDouble() / total
            total == 0 || failureRate <= failureThreshold
        }
        val unhealthyJobs = jobIds.size - healthyJobs
        return mapOf(
            "totalJobs" to jobIds.size,
            "healthyJobs" to healthyJobs,
            "unhealthyJobs" to unhealthyJobs,
            "healthyJobIds" to jobIds.filter { jobId ->
                val jobEntries = getEntriesForJob(jobId)
                val total = jobEntries.size
                val failures = jobEntries.count { it.result is JobResult.Failure }
                val failureRate = if (total == 0) 0.0 else failures.toDouble() / total
                total == 0 || failureRate <= failureThreshold
            }.sorted(),
            "unhealthyJobIds" to jobIds.filter { jobId ->
                val jobEntries = getEntriesForJob(jobId)
                val total = jobEntries.size
                val failures = jobEntries.count { it.result is JobResult.Failure }
                val failureRate = if (total == 0) 0.0 else failures.toDouble() / total
                total > 0 && failureRate > failureThreshold
            }.sorted(),
            "healthRate" to if (jobIds.isEmpty()) 1.0 else healthyJobs.toDouble() / jobIds.size,
            "failureThreshold" to failureThreshold,
            "totalExecutionsInWindow" to entries.size,
            "totalProcessed" to totalProcessed.get()
        )
    }

    /**
     * Alias for [getJobHealthSummary], matching the aggregate naming style used
     * by other summary APIs.
     */
    fun getHealthSummary(jobId: String, failureThreshold: Double = 0.5): Map<String, Any> =
        getJobHealthSummary(jobId, failureThreshold)

    /**
     * Alias for [getOverallHealthSummary].
     */
    fun getHealthSummary(failureThreshold: Double = 0.5): Map<String, Any> =
        getOverallHealthSummary(failureThreshold)

    fun clear() {
        logs.clear()
        totalProcessed.set(0)
    }

    data class JobAuditSummary(
        val jobId: String,
        val totalExecutionsInWindow: Int,
        val successCount: Int,
        val failureCount: Int,
        val averageDurationMs: Double,
        /**
         * The fraction of recorded executions for this job that succeeded.
         */
        val successRate: Double = 0.0,
        /**
         * The fraction of recorded executions for this job that failed.
         */
        val failureRate: Double = 0.0,
        /**
         * The most frequently recorded result type for this job in the current
         * log window, or null when no executions have been recorded.
         */
        val mostCommonResultType: Class<out JobResult>? = null,
        /**
         * Recorded result counts for this job, keyed by result type.
         */
        val resultTypeCounts: Map<Class<out JobResult>, Int> = emptyMap()
    )
}

/**
 * An event listener that automatically records job outcomes to the JobExecutionAuditLog.
 */
class AuditLogEventListener(private val auditLog: JobExecutionAuditLog) : JobEventListener {
    override fun onEvent(event: JobEvent) {
        // We record terminal states (COMPLETED/FAILED) as they provide the final result and duration
        if (event.type == JobEvent.Type.COMPLETED || event.type == JobEvent.Type.FAILED) {
            val execId = event.executionId ?: "unknown"
            val result = event.result ?: JobResult.Failure(RuntimeException("Missing result for terminal event"))
            auditLog.record(
                jobId = event.jobId,
                executionId = execId,
                result = result,
                durationMs = event.durationMs
            )
        }
    }
}
