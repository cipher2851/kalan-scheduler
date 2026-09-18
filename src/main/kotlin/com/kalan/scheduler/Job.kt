package com.kalan.scheduler

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class Job(
    val id: String,
    @Volatile var action: (String) -> Any?,
    val startTime: Instant,
    val intervalMs: Long? = null,
    @Volatile var priority: Int = 0,
    @Volatile var timeoutMs: Long? = null,
    val tags: Set<String> = emptySet(),
    val metadata: Map<String, Any> = emptyMap(),
    val maxRepetitions: Int? = null,
    val dependsOn: String? = null,
    val retryPolicy: RetryPolicy? = null,
    val concurrencyLimit: Int? = null
) : Comparable<Job> {
    private val executionCount = AtomicInteger(0)
    private val failureCount = AtomicInteger(0)
    private val lastExecutionTime = AtomicReference<Instant?>(null)
    private val paused = AtomicBoolean(false)
    private val completed = AtomicBoolean(false)
    private val lastResult = AtomicReference<Any?>(null)
    private val activeExecutions = AtomicInteger(0)

    fun setPaused(paused: Boolean) {
        this.paused.set(paused)
    }

    fun isPaused(): Boolean = paused.get()

    fun markCompleted() {
        completed.set(true)
    }

    fun isCompleted(): Boolean = completed.get()

    fun incrementFailure() {
        failureCount.incrementAndGet()
    }

    fun getFailureCount(): Int = failureCount.get()

    fun tryAcquireSlot(): Boolean {
        if (concurrencyLimit == null) return true
        while (true) {
            val current = activeExecutions.get()
            if (current >= concurrencyLimit!!) return false
            if (activeExecutions.compareAndSet(current, current + 1)) return true
        }
    }

    fun releaseSlot() {
        if (concurrencyLimit != null) {
            activeExecutions.decrementAndGet()
        }
    }

    fun execute(): JobResult {
        if (paused.get()) return JobResult.Paused
        
        if (isMaxRepetitionsReached()) {
            return JobResult.MaxRepetitionsReached
        }

        val executionId = UUID.randomUUID().toString()
        val result = action(executionId)
        
        lastResult.set(result)
        executionCount.incrementAndGet()
        lastExecutionTime.set(Instant.now())
        
        if (intervalMs == null) {
            markCompleted()
        }
        return JobResult.Success(result)
    }

    fun getExecutionCount(): Int = executionCount.get()
    
    fun getLastExecutionTime(): Instant? = lastExecutionTime.get()

    fun getLastResult(): Any? = lastResult.get()

    fun isMaxRepetitionsReached(): Boolean {
        return maxRepetitions != null && executionCount.get() >= maxRepetitions!!
    }

    override fun compareTo(other: Job): Int {
        return other.priority.compareTo(this.priority) // Higher priority first
    }
}

data class RetryPolicy(
    val maxRetries: Int,
    val delayMs: Long
)

sealed class JobResult {
    data class Success(val value: Any?) : JobResult()
    object Paused : JobResult()
    object MaxRepetitionsReached : JobResult()
    data class Failure(val exception: Throwable) : JobResult()
    object ConcurrencyLimitReached : JobResult()
}