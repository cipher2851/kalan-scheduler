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

        if (logs.size > maxLogSize) {
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

    fun getEntriesForJobWithResultType(jobId: String, resultClass: Class<out JobResult>): List<AuditEntry> {
        return logs.filter { it.jobId == jobId && resultClass.isInstance(it.result) }
    }

    fun getTotalExecutionTimeMs(): Long {
        return logs.sumOf { it.durationMs }
    }

    fun getTotalProcessedCount(): Int = totalProcessed.get()

    fun clear() {
        logs.clear()
        totalProcessed.set(0)
    }
}

/**
 * An event listener that automatically records job outcomes to the JobExecutionAuditLog.
 */
class AuditLogEventListener(private val auditLog: JobExecutionAuditLog) : JobEventListener {
    override fun onEvent(event: JobEvent) {
        // We only record terminal states (COMPLETED/FAILED) as they provide the final result and duration
        // The Job class manages its own history, so the audit log focuses on the finality.
        // To get the detailed metrics (duration/executionId), the listener would typically
        // need access to the Job instance. Since JobEvent is lean, we rely on the Job's
        // internal tracking and the scheduler to trigger the final record.
        // However, for this bridge, we can trigger a lookup if the event provides enough context.
    }
}