package com.kalan.scheduler

import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.pow

/**
 * Runtime context provided to a job during its execution.
 * Provides access to both metadata and the current execution's state.
 */
class JobExecutionContext(
    val executionId: String,
    val jobId: String,
    val startTime: Instant,
    val metadata: Map<String, Any> = emptyMap(),
    private val job: Job
) {
    val isCancelled: Boolean get() = job.getJobStatus() == JobExecutionStatus.PAUSED
    
    fun getStatistics(): JobStatistics = job.getStatistics()
    
    fun getExecutionCount(): Int = job.getExecutionCount()
}

/**
 * Interceptor for job execution. Allows running logic before and after the job action.
 */
interface JobExecutionInterceptor {
    fun beforeExecute(context: JobExecutionContext): Boolean // return false to abort execution
    fun afterExecute(context: JobExecutionContext, result: JobResult)
}

/**
 * Interceptor that limits the number of concurrent executions across multiple jobs.
 */
class JobExecutionLimitInterceptor(private val maxConcurrent: Int) : JobExecutionInterceptor {
    private val activeCount = AtomicInteger(0)

    override fun beforeExecute(context: JobExecutionContext): Boolean {
        while (true) {
            val current = activeCount.get()
            if (current >= maxConcurrent) return false
            if (activeCount.compareAndSet(current, current + 1)) return true
        }
    }

    override fun afterExecute(context: JobExecutionContext, result: JobResult) {
        activeCount.decrementAndGet()
    }
}

/**
 * Interceptor that throttles the rate of job executions using a token bucket algorithm.
 */
class JobThrottlingInterceptor(private val throttler: JobThrottler) : JobExecutionInterceptor {
    override fun beforeExecute(context: JobExecutionContext): Boolean {
        return throttler.tryAcquire()
    }

    override fun afterExecute(context: JobExecutionContext, result: JobResult) {}
}

/**
 * Interceptor for logging job execution lifecycle.
 */
class LoggingInterceptor : JobExecutionInterceptor {
    private val logger = java.util.logging.Logger.getLogger(LoggingInterceptor::class.java.name)

    override fun beforeExecute(context: JobExecutionContext): Boolean {
        logger.info("Executing job ${context.jobId} (Execution ID: ${context.executionId})")
        return true
    }

    override fun afterExecute(context: JobExecutionContext, result: JobResult) {
        val status = when (result) {
            is JobResult.Success -> "SUCCESS"
            is JobResult.Failure -> "FAILED (${result.exception.message})"
            else -> result.javaClass.simpleName
        }
        logger.info("Job ${context.jobId} finished with status: $status")
    }
}

/**
 * Implements a Token Bucket algorithm to limit the rate of job executions.
 */
class JobThrottler(private val maxTokens: Double, private val refillRatePerSecond: Double) {
    private var tokens = maxTokens
    private var lastRefillTime = System.nanoTime()

    @Synchronized
    fun tryAcquire(): Boolean {
        refill()
        return if (tokens >= 1.0) {
            tokens -= 1.0
            true
        } else {
            false
        }
    }

    private fun refill() {
        val now = System.nanoTime()
        val deltaSeconds = (now - lastRefillTime).toDouble() / 1_000_000_000.0
        tokens = (tokens + deltaSeconds * refillRatePerSecond).coerceAtMost(maxTokens)
        lastRefillTime = now
    }
}

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
    val failureRate: Double,
    val averageDurationMs: Double,
    val totalDurationMs: Long,
    val lastExecutionTime: Instant?,
    val currentStatus: JobExecutionStatus,
    val lastDurationMs: Long,
    val windowedFailureRate: Double,
    val windowedSuccessRate: Double,
    val rollingAverageDurationMs: Double
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
    val totalDurationMs = AtomicLong(0)
    val totalFailureDurationMs = AtomicLong(0)
    val lastExecutionTime = AtomicReference<Instant?>(null)
    val paused = AtomicBoolean(false)
    val completed = AtomicBoolean(false)
    val lastResult = AtomicReference<JobResult?>(null)
    val activeExecutions = AtomicInteger(0)
    val status = AtomicReference<JobExecutionStatus>(JobExecutionStatus.QUEUED)
    val currentExecutionId = AtomicReference<String?>(null)
    val lastDurationMs = AtomicLong(0)
    
    val startedEvents = AtomicInteger(0)
    val completedEvents = AtomicInteger(0)
    val failedEvents = AtomicInteger(0)
    val lastEventType = AtomicReference<JobEvent.Type?>(null)

    private val recentOutcomes = ConcurrentLinkedDeque<Boolean>()
    private val windowSize = 50

    fun updateStatus(status: JobExecutionStatus) {
        this.status.set(status)
    }

    fun recordAttempt(success: Boolean, result: JobResult? = null, durationMs: Long = 0) {
        totalAttempts.incrementAndGet()
        totalDurationMs.addAndGet(durationMs)
        lastDurationMs.set(durationMs)
        
        recentOutcomes.addFirst(success)
        if (recentOutcomes.size > windowSize) {
            recentOutcomes.removeLast()
        }

        if (success) {
            successCount.incrementAndGet()
            executionCount.incrementAndGet()
            lastExecutionTime.set(Instant.now())
        } else {
            failureCount.incrementAndGet()
            totalFailureDurationMs.addAndGet(durationMs)
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

    fun getWindowedFailureRate(): Double {
        if (recentOutcomes.isEmpty()) return 0.0
        val failures = recentOutcomes.count { !it }
        return failures.toDouble() / recentOutcomes.size
    }

    fun getWindowedSuccessRate(): Double {
        if (recentOutcomes.isEmpty()) return 0.0
        val successes = recentOutcomes.count { it }
        return successes.toDouble() / recentOutcomes.size
    }

    /**
     * Calculates a rolling average of the last N durations provided.
     * This helps in understanding recent performance trends.
     */
    fun calculateRollingAverage(history: List<JobExecutionRecord>, windowSize: Int = 10): Double {
        val recent = history.take(windowSize)
        if (recent.isEmpty()) return 0.0
        return recent.map { it.durationMs }.average()
    }
}

class Job(
    val id: String,
    @Volatile var action: (JobExecutionContext) -> Any?,
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
    private val resultListeners = CopyOnWriteArrayList<JobResultListener>()
    private val outcomeListeners = CopyOnWriteArrayList<JobOutcomeListener>()
    private val interceptors = CopyOnWriteArrayList<JobExecutionInterceptor>()

    fun addEventListener(listener: JobEventListener) {
        listeners.add(listener)
    }

    fun removeEventListener(listener: JobEventListener) {
        listeners.remove(listener)
    }

    fun addResultListener(listener: JobResultListener) {
        resultListeners.add(listener)
    }

    fun removeResultListener(listener: JobResultListener) {
        resultListeners.remove(listener)
    }

    fun addOutcomeListener(listener: JobOutcomeListener) {
        outcomeListeners.add(listener)
    }

    fun removeOutcomeListener(listener: JobOutcomeListener) {
        outcomeListeners.remove(listener)
    }

    fun addInterceptor(interceptor: JobExecutionInterceptor) {
        interceptors.add(interceptor)
    }

    fun addInterceptors(vararg interceptorsToAdd: JobExecutionInterceptor) {
        interceptors.addAll(interceptorsToAdd)
    }

    fun removeInterceptor(interceptor: JobExecutionInterceptor) {
        interceptors.remove(interceptor)
    }

    private fun notifyListeners(event: JobEvent) {
        lifecycle.recordEvent(event.type)
        listeners.forEach { it.onEvent(event) }
    }

    private fun notifyResultListeners(result: JobResult) {
        resultListeners.forEach { it.onResult(id, result) }
    }

    private fun notifyOutcomeListeners(result: JobResult) {
        outcomeListeners.forEach { it.onOutcome(id, result) }
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
        // Failure counts are recorded by JobLifecycleManager.recordAttempt().
        // This only moves the job to its terminal failed status.
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

        val activeInterceptors = interceptors.toList()
        val passedInterceptors = mutableListOf<JobExecutionInterceptor>()

        var currentResult: JobResult
        var attempts = 0
        val maxAttempts = (retryPolicy?.maxRetries ?: 0) + 1
        val start = Instant.now()
        val executionId = UUID.randomUUID().toString()
        val context = JobExecutionContext(executionId, id, start, metadata, this)
        
        try {
            lifecycle.updateStatus(JobExecutionStatus.RUNNING)
            lifecycle.currentExecutionId.set(executionId)
            
            notifyListeners(JobEvent(JobEvent.Type.STARTED, id, executionId))

            // Interceptor: Before execute
            for (interceptor in activeInterceptors) {
                val allowed = try {
                    interceptor.beforeExecute(context)
                } catch (t: Throwable) {
                    passedInterceptors.forEach { ip ->
                        try {
                            ip.afterExecute(context, JobResult.Failure(t))
                        } catch (_: Throwable) {
                            // Ignore cleanup failures from previously passed interceptors.
                        }
                    }
                    try {
                        interceptor.afterExecute(context, JobResult.Failure(t))
                    } catch (_: Throwable) {
                        // Ignore cleanup failures from the throwing interceptor.
                    }
                    throw t
                }
                if (!allowed) {
                    val abortedResult = JobResult.Skipped
                    passedInterceptors.forEach { ip ->
                        try {
                            ip.afterExecute(context, abortedResult)
                        } catch (_: Throwable) {
                            // Ignore cleanup failures while aborting.
                        }
                    }
                    return abortedResult
                }
                passedInterceptors.add(interceptor)
            }

            do {
                attempts++
                
                val attemptStart = Instant.now()
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
                    } else if (customExecutor != null) {
                        val future = CompletableFuture.supplyAsync({ action(context) }, customExecutor)
                        try {
                            future.join()
                        } catch (e: java.util.concurrent.CompletionException) {
                            throw e.cause ?: e
                        }
                    } else {
                        action(context)
                    }
                    
                    val duration = java.time.Duration.between(attemptStart, Instant.now()).toMillis()
                    lifecycle.recordAttempt(success = true, result = JobResult.Success(res), durationMs = duration)
                    
                    if (intervalMs == null) {
                        markCompleted()
                        resultFuture.complete(res)
                    }
                    JobResult.Success(res)
                } catch (e: Throwable) {
                    val duration = java.time.Duration.between(attemptStart, Instant.now()).toMillis()
                    lifecycle.recordAttempt(success = false, result = JobResult.Failure(e), durationMs = duration)
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

            // Interceptor: After execute
            passedInterceptors.forEach { ip ->
                try {
                    ip.afterExecute(context, currentResult)
                } catch (_: Throwable) {
                    // Ignore cleanup failures from an interceptor.
                }
            }

            notifyResultListeners(currentResult)
            notifyOutcomeListeners(currentResult)
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
        val avgDuration = if (total > 0) lifecycle.totalDurationMs.get().toDouble() / total else 0.0
        
        return JobStatistics(
            totalAttempts = total,
            successCount = success,
            failureCount = failure,
            successRate = if (total > 0) success.toDouble() / total else 0.0,
            failureRate = if (total > 0) failure.toDouble() / total else 0.0,
            averageDurationMs = avgDuration,
            totalDurationMs = lifecycle.totalDurationMs.get(),
            lastExecutionTime = lifecycle.lastExecutionTime.get(),
            currentStatus = lifecycle.status.get(),
            lastDurationMs = lifecycle.lastDurationMs.get(),
            windowedFailureRate = lifecycle.getWindowedFailureRate(),
            windowedSuccessRate = lifecycle.getWindowedSuccessRate(),
            rollingAverageDurationMs = lifecycle.calculateRollingAverage(history.toList())
        )
    }

    /**
     * Returns a health status based on success/failure ratio.
     * Consider a job unhealthy if failure rate exceeds 50% after at least 2 attempts.
     */
    fun getHealthStatus(): JobHealth {
        val total = lifecycle.totalAttempts.get()
        val failure = lifecycle.failureCount.get()
        val failureRate = if (total > 0) failure.toDouble() / total else 0.0
        
        return if (total >= 2 && failureRate > 0.5) {
            JobHealth.UNHEALTHY
        } else if (total == 0) {
            JobHealth.UNKNOWN
        } else {
            JobHealth.HEALTHY
        }
    }

    override fun compareTo(other: Job): Int {
        val priorityCompare = other.priority.compareTo(this.priority) // Higher priority first
        return if (priorityCompare != 0) priorityCompare else id.compareTo(other.id)
    }
}

enum class JobHealth {
    HEALTHY, UNHEALTHY, UNKNOWN
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

/**
 * Listener specifically for job completion results.
 */
interface JobResultListener {
    fun onResult(jobId: String, result: JobResult)
}

/**
 * Listener for job outcomes, allowing decoupled handling of success/failure.
 */
interface JobOutcomeListener {
    fun onOutcome(jobId: String, result: JobResult)
}
