package com.kalan.scheduler

import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.ConcurrentLinkedDeque

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
 * Encapsulates the runtime state of a Job.
 */
class JobState {
    val executionCount = AtomicInteger(0)
    val failureCount = AtomicInteger(0)
    val lastExecutionTime = AtomicReference<Instant?>(null)
    val paused = AtomicBoolean(false)
    val completed = AtomicBoolean(false)
    val lastResult = AtomicReference<Any?>(null)
    val activeExecutions = AtomicInteger(0)
    val status = AtomicReference<JobExecutionStatus>(JobExecutionStatus.QUEUED)
}

class Job(
    val id: String,
    @Volatile var action: (String) -> Any?,
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
    private val state = JobState()
    private val resultFuture = CompletableFuture<Any?>()
    private val history = ConcurrentLinkedDeque<JobExecutionRecord>()

    fun setPaused(paused: Boolean) {
        state.paused.set(paused)
        state.status.set(if (paused) JobExecutionStatus.PAUSED else JobExecutionStatus.QUEUED)
    }

    fun isPaused(): Boolean = state.paused.get()

    fun markCompleted() {
        state.completed.set(true)
        state.status.set(JobExecutionStatus.COMPLETED)
    }

    fun isCompleted(): Boolean = state.completed.get()

    fun incrementFailure() {
        state.failureCount.incrementAndGet()
        state.status.set(JobExecutionStatus.FAILED)
    }

    fun getFailureCount(): Int = state.failureCount.get()

    fun tryAcquireSlot(): Boolean {
        if (concurrencyLimit == null) return true
        while (true) {
            val current = state.activeExecutions.get()
            if (current >= concurrencyLimit!!) return false
            if (state.activeExecutions.compareAndSet(current, current + 1)) return true
        }
    }

    fun releaseSlot() {
        if (concurrencyLimit != null) {
            state.activeExecutions.decrementAndGet()
        }
    }

    fun execute(): JobResult {
        if (state.paused.get()) return JobResult.Paused
        
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

        val executionId = UUID.randomUUID().toString()
        val start = Instant.now()
        state.status.set(JobExecutionStatus.RUNNING)
        
        val result = try {
            val res = action(executionId)
            
            state.lastResult.set(res)
            state.executionCount.incrementAndGet()
            state.lastExecutionTime.set(Instant.now())
            
            if (intervalMs == null) {
                markCompleted()
                resultFuture.complete(res)
            }
            JobResult.Success(res)
        } catch (e: Throwable) {
            incrementFailure()
            if (intervalMs == null) {
                resultFuture.completeExceptionally(e)
            }
            JobResult.Failure(e)
        } finally {
            releaseSlot()
        }

        val end = Instant.now()
        val duration = java.time.Duration.between(start, end).toMillis()
        history.addFirst(JobExecutionRecord(executionId, start, end, result, duration))
        if (history.size > 100) history.removeLast()

        return result
    }

    fun getExecutionCount(): Int = state.executionCount.get()
    
    fun getLastExecutionTime(): Instant? = state.lastExecutionTime.get()

    fun getLastResult(): Any? = state.lastResult.get()

    fun isMaxRepetitionsReached(): Boolean {
        return maxRepetitions != null && state.executionCount.get() >= maxRepetitions!!
    }

    fun getResultFuture(): CompletableFuture<Any?> = resultFuture

    fun getHistory(): List<JobExecutionRecord> = history.toList()

    @Suppress("UNCHECKED_CAST")
    fun <T> getMetadataValue(key: String): T? = metadata[key] as? T

    fun getJobStatus(): JobExecutionStatus = state.status.get()

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