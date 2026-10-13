package com.kalan.scheduler

import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.pow

/**
 * Context provided to a job during its execution.
 */
data class JobContext(
    val executionId: String,
    val jobId: String,
    val startTime: Instant,
    val metadata: Map<String, Any> = emptyMap()
)

/**
 * Represents the priority of a job. Higher values indicate higher priority.
 */
@JvmInline
value class JobPriority(val value: Int) : Comparable<JobPriority> {
    override fun compareTo(other: JobPriority): Int = this.value.compareTo(other.value)

    companion object {
        val LOW = JobPriority(0)
        val NORMAL = JobPriority(10)
        val HIGH = JobPriority(20)
        val CRITICAL = JobPriority(30)
    }
}

enum class JobExecutionStrategy {
    QUEUE,            // Allow multiple executions to queue up
    SKIP_IF_RUNNING    // Skip the execution if a previous one is still active
}

data class JobExecutionRecord(
    val executionId: String,
    val startTime: Instant,
    val endTime: Instant,
    val result: JobResult,
    val durationMs: Long
)

enum class JobExecutionStatus {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    PAUSED
}

/**
 * Structured statistics for a job's lifetime performance.
 */
data class JobStatistics(
    val totalAttempts: Int,
    val successCount: Int,
    val failureCount: Int,
    val successRate: Double,
    val averageDurationMs: Double,
    val lastExecutionTime: Instant?,
    val currentStatus: JobExecutionStatus
)

/**
 * Aggregated summary of events for a job.
 */
data class JobEventSummary(
    val startedCount: Int,
    val completedCount: Int,
    val failedCount: Int,
    val lastEventType: JobEvent.Type? = null
)

/**
 * Manages the runtime state, events and statistics of a Job.
 */
class JobLifecycleManager {
    val executionCount = AtomicInteger(0)
    val totalAttempts = AtomicInteger(0)
    val successCount = AtomicInteger(0)
    val failureCount = AtomicInteger(0)
    val lastExecutionTime = AtomicReference<Instant?>(null)
    val paused = AtomicBoolean(false)
    val completed = AtomicBoolean(false)
    val lastResult = AtomicReference<JobResult?>(null)
    val activeExecutions = AtomicInteger(0)
    val status = AtomicReference<JobExecutionStatus>(JobExecutionStatus.QUEUED)
    val currentExecutionId = AtomicReference<String?>(null)
    
    val startedEvents = AtomicInteger(0)
    val completedEvents = AtomicInteger(0)
    val failedEvents = AtomicInteger(0)
    val lastEventType = AtomicReference<JobEvent.Type?>(null)

    fun updateStatus(status: JobExecutionStatus) {
        this.status.set(status)
    }

    fun recordAttempt(success: Boolean, result: JobResult? = null) {
        totalAttempts.incrementAndGet()
        if (success) {
            successCount.incrementAndGet()
            executionCount.incrementAndGet()
            lastExecutionTime.set(Instant.now())
        } else {
            failureCount.incrementAndGet()
        }
        lastResult.set(result)
    }

    fun recordEvent(type: JobEvent.Type) {
        lastEventType.set(type)
        when (type) {
            JobEvent.Type.STARTED -> startedEvents.incrementAndGet()
            JobEvent.Type.COMPLETED -> completedEvents.incrementAndGet()
            JobEvent.Type.FAILED -> failedEvents.incrementAndGet()
        }
    }
}

class Job(
    val id: String,
    @Volatile var action: (JobContext) -> Any?,
    val startTime: Instant,
    val intervalMs: Long? = null,
    @Volatile var priority: JobPriority = JobPriority.NORMAL,
    @Volatile var timeoutMs: Long? = null,
    val tags: Set<String> = emptySet(),
    val metadata: Map<String, Any> = emptyMap(),
    val maxRepetitions: Int? = null,
    val dependsOn: String? = null,
    val retryPolicy: RetryPolicy? = null,
    val concurrencyLimit: Int? = null,
    @Volatile var executionStrategy: JobExecutionStrategy = JobExecutionStrategy.QUEUE,
    val customExecutor: Executor? = null
) : Comparable<Job> {
    private val lifecycle = JobLifecycleManager()
    private val resultFuture = CompletableFuture<Any?>()
    private val history = ConcurrentLinkedDeque<JobExecutionRecord>()
    private val listeners = CopyOnWriteArrayList<JobEventListener>()

    fun addEventListener(listener: JobEventListener) {
        listeners.add(listener)
    }

    fun removeEventListener(listener: JobEventListener) {
        listeners.remove(listener)
    }

    private fun notifyListeners(event: JobEvent) {
        lifecycle.recordEvent(event.type)
        listeners.forEach { it.onEvent(event) }
    }

    fun setPaused(paused: Boolean) {
        lifecycle.paused.set(paused)
        lifecycle.updateStatus(if (paused) JobExecutionStatus.PAUSED else JobExecutionStatus.QUEUED)
    }

    fun isPaused(): Boolean = lifecycle.paused.get()

    fun markCompleted() {
        lifecycle.completed.set(true)
        lifecycle.updateStatus(JobExecutionStatus.COMPLETED)
    }

    fun isCompleted(): Boolean = lifecycle.completed.get()

    fun incrementFailure() {
        lifecycle.failureCount.incrementAndGet()
        lifecycle.updateStatus(JobExecutionStatus.FAILED)
    }

    fun getFailureCount(): Int = lifecycle.failureCount.get()
    fun getSuccessCount(): Int = lifecycle.successCount.get()

    fun tryAcquireSlot(): Boolean {
        if (concurrencyLimit == null) return true
        while (true) {
            val current = lifecycle.activeExecutions.get()
            if (current >= concurrencyLimit!!) return false
            if (lifecycle.activeExecutions.compareAndSet(current, current + 1)) return true
        }
    }

    fun releaseSlot() {
        if (concurrencyLimit != null) {
            lifecycle.activeExecutions.decrementAndGet()
        }
    }

    fun execute(): JobResult {
        if (lifecycle.paused.get()) return JobResult.Paused
        
        if (isMaxRepetitionsReached()) {
            return JobResult.MaxRepetitionsReached
        }

        if (!tryAcquireSlot()) {
            return if (executionStrategy == JobExecutionStrategy.SKIP_IF_RUNNING) {
                JobResult.Skipped
            } else {
                JobResult.ConcurrencyLimitReached
            }
        }

        var currentResult: JobResult
        var attempts = 0
        val maxAttempts = (retryPolicy?.maxRetries ?: 0) + 1
        val start = Instant.now()
        val executionId = UUID.randomUUID().toString()
        val context = JobContext(executionId, id, start, metadata)
        
        try {
            lifecycle.updateStatus(JobExecutionStatus.RUNNING)
            lifecycle.currentExecutionId.set(executionId)
            
            notifyListeners(JobEvent(JobEvent.Type.STARTED, id, executionId))

            do {
                attempts++
                lifecycle.totalAttempts.incrementAndGet()
                
                currentResult = try {
                    val res = if (timeoutMs != null) {
                        val timeoutFuture = CompletableFuture.supplyAsync({ action(context) }, customExecutor)
                        try {
                            timeoutFuture.get(timeoutMs!!, TimeUnit.MILLISECONDS)
                        } catch (e: TimeoutException) {
                            timeoutFuture.cancel(true)
                            throw e
                        } catch (e: java.util.concurrent.ExecutionException) {
                            throw e.cause ?: e
                        }
                    } else {
                        action(context)
                    }
                    
                    lifecycle.recordAttempt(success = true, result = JobResult.Success(res))
                    
                    if (intervalMs == null) {
                        markCompleted()
                        resultFuture.complete(res)
                    }
                    JobResult.Success(res)
                } catch (e: Throwable) {
                    incrementFailure()
                    if (intervalMs == null && attempts >= maxAttempts) {
                        resultFuture.completeExceptionally(e)
                    }
                    JobResult.Failure(e)
                }

                if (currentResult is JobResult.Failure && attempts < maxAttempts) {
                    val delay = calculateRetryDelay(attempts)
                    try {
                        Thread.sleep(delay)
                    } catch (ie: InterruptedException) {
                        Thread.currentThread().interrupt()
                        currentResult = JobResult.Failure(ie)
                        break
                    }
                }
            } while (currentResult is JobResult.Failure && attempts < maxAttempts)

            lifecycle.currentExecutionId.set(null)
            lifecycle.lastResult.set(currentResult)
            
            val end = Instant.now()
            val duration = java.time.Duration.between(start, end).toMillis()
            history.addFirst(JobExecutionRecord(executionId, start, end, currentResult, duration))
            if (history.size > 100) history.removeLast()

            if (currentResult is JobResult.Success) {
                notifyListeners(JobEvent(JobEvent.Type.COMPLETED, id, executionId, currentResult, duration))
            } else if (currentResult is JobResult.Failure) {
                notifyListeners(JobEvent(JobEvent.Type.FAILED, id, executionId, currentResult, duration, (currentResult as JobResult.Failure).exception))
            }

            return currentResult
        } finally {
            releaseSlot()
        }
    }

    private fun calculateRetryDelay(attempt: Int): Long {
        val policy = retryPolicy ?: return 0L
        return if (policy.useExponentialBackoff) {
            (policy.delayMs * 2.0.pow(attempt - 1).toLong())
        } else {
            policy.delayMs
        }
    }

    fun getExecutionCount(): Int = lifecycle.executionCount.get()
    fun getTotalAttempts(): Int = lifecycle.totalAttempts.get()
    
    fun getLastExecutionTime(): Instant? = lifecycle.lastExecutionTime.get()

    fun getLastResult(): Any? {
        val res = lifecycle.lastResult.get()
        return if (res is JobResult.Success) res.value else null
    }

    fun getLastJobResult(): JobResult? = lifecycle.lastResult.get()

    fun isMaxRepetitionsReached(): Boolean {
        return maxRepetitions != null && lifecycle.executionCount.get() >= maxRepetitions!!
    }

    fun getResultFuture(): CompletableFuture<Any?> = resultFuture

    fun getHistory(): List<JobExecutionRecord> = history.toList()

    @Suppress("UNCHECKED_CAST")
    fun <T> getMetadataValue(key: String): T? = metadata[key] as? T

    fun getJobStatus(): JobExecutionStatus = lifecycle.status.get()

    fun getCurrentExecutionId(): String? = lifecycle.currentExecutionId.get()

    fun getEventSummary(): JobEventSummary {
        return JobEventSummary(
            startedCount = lifecycle.startedEvents.get(),
            completedCount = lifecycle.completedEvents.get(),
            failedCount = lifecycle.failedEvents.get(),
            lastEventType = lifecycle.lastEventType.get()
        )
    }

    fun getStatistics(): JobStatistics {
        val total = lifecycle.totalAttempts.get()
        val success = lifecycle.successCount.get()
        val failure = lifecycle.failureCount.get()
        val avgDuration = history.map { it.durationMs }.average().takeIf { !it.isNaN() } ?: 0.0
        
        return JobStatistics(
            totalAttempts = total,
            successCount = success,
            failureCount = failure,
            successRate = if (total > 0) success.toDouble() / total else 0.0,
            averageDurationMs = avgDuration,
            lastExecutionTime = lifecycle.lastExecutionTime.get(),
            currentStatus = lifecycle.status.get()
        )
    }

    override fun compareTo(other: Job): Int {
        return other.priority.compareTo(this.priority) // Higher priority first
    }
}

data class RetryPolicy(
    val maxRetries: Int,
    val delayMs: Long,
    val useExponentialBackoff: Boolean = false
)

sealed class JobResult {
    data class Success(val value: Any?) : JobResult()
    data object Paused : JobResult()
    data object MaxRepetitionsReached : JobResult()
    data class Failure(val exception: Throwable) : JobResult()
    data object ConcurrencyLimitReached : JobResult()
    data object Skipped : JobResult()
}

/**
 * Interface for creating Job instances from persisted data.
 */
interface JobFactory {
    fun createJob(id: String, priority: Int, tags: Set<String>): Job
}

/**
 * Event emitted by a Job during its lifecycle.
 */
data class JobEvent(
    val type: Type,
    val jobId: String,
    val executionId: String? = null,
    val result: JobResult? = null,
    val durationMs: Long = 0,
    val exception: Throwable? = null
) {
    enum class Type {
        STARTED,
        COMPLETED,
        FAILED
    }
}

/**
 * Listener for Job lifecycle events.
 */
interface JobEventListener {
    fun onEvent(event: JobEvent)
}
