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

    fun getTotalProcessedCount(): Int = totalProcessed.get()

    fun clear() {
        logs.clear()
        totalProcessed.set(0)
    }
}