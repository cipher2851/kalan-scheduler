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

    fun getTotalExecutionTimeMs(): Long {
        return logs.sumOf { it.durationMs }
    }

    fun getTotalExecutionTimeForResult(resultClass: Class<out JobResult>): Long {
        return logs.filter { resultClass.isInstance(it.result) }.sumOf { it.durationMs }
    }

    fun getTotalProcessedCount(): Int = totalProcessed.get()

    /**
     * Provides a summary of audit records for a specific job within the current log window.
     */
    fun getJobAuditSummary(jobId: String): JobAuditSummary {
        val jobEntries = getEntriesForJob(jobId)
        val total = jobEntries.size
        val successes = jobEntries.count { it.result is JobResult.Success }
        val failures = jobEntries.count { it.result is JobResult.Failure }
        val avgDuration = if (total > 0) jobEntries.map { it.durationMs }.average() else 0.0
        
        return JobAuditSummary(
            jobId = jobId,
            totalExecutionsInWindow = total,
            successCount = successes,
            failureCount = failures,
            averageDurationMs = avgDuration
        )
    }

    fun clear() {
        logs.clear()
        totalProcessed.set(0)
    }

    data class JobAuditSummary(
        val jobId: String,
        val totalExecutionsInWindow: Int,
        val successCount: Int,
        val failureCount: Int,
        val averageDurationMs: Double
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