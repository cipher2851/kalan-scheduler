package com.kalan.scheduler

import java.time.Duration
import java.time.Instant
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

class KalanScheduler(corePoolSize: Int = 1, threadFactory: ThreadFactory = DefaultKalanThreadFactory()) {
    private val scheduler = ScheduledThreadPoolExecutor(corePoolSize, threadFactory)
    private val activeJobs = ConcurrentHashMap<String, ScheduledFuture<*>>()
    private val jobInstances = ConcurrentHashMap<String, Job>()
    private val running = AtomicBoolean(true)
    
    var errorHandler: (Throwable) -> Unit = { it.printStackTrace() }

    private fun wrapExecution(job: Job, action: () -> Unit): () -> Unit {
        return {
            val timeout = job.timeoutMs
            if (timeout == null) {
                try {
                    action()
                } catch (e: Throwable) {
                    errorHandler(e)
                }
            } else {
                val future = CompletableFuture.runAsync({ 
                    try {
                        action()
                    } catch (e: Throwable) {
                        throw e
                    }
                }, scheduler)
                
                try {
                    future.get(timeout, TimeUnit.MILLISECONDS)
                } catch (e: TimeoutException) {
                    future.cancel(true)
                    errorHandler(TimeoutException("Job ${job.id} timed out after $timeout ms"))
                } catch (e: ExecutionException) {
                    errorHandler(e.cause ?: e)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    errorHandler(e)
                } catch (e: Throwable) {
                    errorHandler(e)
                }
            }
        }
    }

    fun schedule(id: String, delayMs: Long, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), dependsOn: String? = null, action: (String) -> Unit) {
        if (!running.get()) return
        
        val job = Job(id, action, Instant.now().plusMillis(delayMs), priority = priority, timeoutMs = timeoutMs, tags = tags, metadata = metadata, dependsOn = dependsOn)
        jobInstances[id] = job

        val wrappedAction = wrapExecution(job) { job.execute() }
        val future = scheduler.schedule({
            try {
                if (job.dependsOn != null && getJobStatus(job.dependsOn) != JobStatus.COMPLETED) {
                    // Dependency not met, reschedule after a short delay
                    schedule(id, 100, priority, timeoutMs, tags, metadata, dependsOn, action)
                    return@schedule
                }
                wrappedAction()
            } catch (e: Throwable) {
                errorHandler(e)
            } finally {
                if (job.intervalMs == null) {
                    jobInstances.remove(id)
                    activeJobs.remove(id)
                }
            }
        }, delayMs, TimeUnit.MILLISECONDS)
        
        activeJobs[id] = future
    }

    fun scheduleAt(id: String, startTime: Instant, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), dependsOn: String? = null, action: (String) -> Unit) {
        val delay = Duration.between(Instant.now(), startTime).toMillis()
        schedule(id, if (delay < 0) 0 else delay, priority, timeoutMs, tags, metadata, dependsOn, action)
    }

    fun scheduleAtFixedRate(id: String, initialDelayMs: Long, periodMs: Long, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), maxRepetitions: Int? = null, action: (String) -> Unit) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now().plusMillis(initialDelayMs), periodMs, priority, timeoutMs, tags, metadata, maxRepetitions)
        jobInstances[id] = job

        val wrappedAction = wrapExecution(job) { 
            val success = job.execute()
            if (!success && job.maxRepetitions != null) {
                cancel(id)
            }
        }
        val future = scheduler.scheduleAtFixedRate({
            try {
                wrappedAction()
            } catch (e: Throwable) {
                errorHandler(e)
            }
        }, initialDelayMs, periodMs, TimeUnit.MILLISECONDS)

        activeJobs[id] = future
    }

    fun scheduleWithFixedDelay(id: String, initialDelayMs: Long, delayMs: Long, priority: Int = 0, timeoutMs: Long? = null, tags: Set<String> = emptySet(), metadata: Map<String, Any> = emptyMap(), maxRepetitions: Int? = null, action: (String) -> Unit) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now().plusMillis(initialDelayMs), delayMs, priority, timeoutMs, tags, metadata, maxRepetitions)
        jobInstances[id] = job

        val wrappedAction = wrapExecution(job) { 
            val success = job.execute()
            if (!success && job.maxRepetitions != null) {
                cancel(id)
            }
        }
        val future = scheduler.scheduleWithFixedDelay({
            try {
                wrappedAction()
            } catch (e: Throwable) {
                errorHandler(e)
            }
        }, initialDelayMs, delayMs, TimeUnit.MILLISECONDS)

        activeJobs[id] = future
    }

    fun updateJob(id: String, newAction: (String) -> Unit) {
        jobInstances[id]?.action = newAction
    }

    fun updateJobPriority(id: String, priority: Int) {
        jobInstances[id]?.priority = priority
    }

    fun updateJobTimeout(id: String, timeoutMs: Long?) {
        jobInstances[id]?.timeoutMs = timeoutMs
    }

    fun pauseJob(id: String) {
        jobInstances[id]?.setPaused(true)
    }

    fun resumeJob(id: String) {
        jobInstances[id]?.setPaused(false)
    }

    fun cancel(id: String) {
        activeJobs.remove(id)?.cancel(false)
        jobInstances.remove(id)
    }

    fun cancelByTag(tag: String) {
        jobInstances.values.filter { it.tags.contains(tag) }
            .map { it.id }
            .forEach { cancel(it) }
    }

    fun isJobActive(id: String): Boolean {
        val future = activeJobs[id]
        return future != null && !future.isDone
    }

    fun getJobStatus(id: String): JobStatus {
        val future = activeJobs[id]
        return when {
            future == null -> JobStatus.NOT_FOUND
            future.isCancelled -> JobStatus.CANCELLED
            future.isDone -> JobStatus.COMPLETED
            else -> JobStatus.RUNNING
        }
    }

    fun getExecutionCount(id: String): Int {
        return jobInstances[id]?.getExecutionCount() ?: 0
    }

    fun getLastExecutionTime(id: String): Instant? {
        return jobInstances[id]?.getLastExecutionTime()
    }

    fun getJobInfo(id: String): JobInfo?
        = jobInstances[id]?.let {
            JobInfo(id, getJobStatus(id), it.getExecutionCount(), it.getLastExecutionTime(), it.intervalMs, it.priority, it.tags, it.metadata)
        }

    fun listAllJobs(): List<JobInfo> {
        return jobInstances.keys.mapNotNull { getJobInfo(it) }
    }

    fun listJobsByTag(tag: String): List<JobInfo> {
        return jobInstances.values.filter { it.tags.contains(tag) }
            .mapNotNull { getJobInfo(it.id) }
    }

    fun shutdown() {
        running.set(false)
        scheduler.shutdownNow()
        jobInstances.clear()
        activeJobs.clear()
    }

    fun getActiveJobCount(): Int = activeJobs.size

    fun getActiveJobIds(): Set<String> = activeJobs.keys.toSet()

    fun scheduleJob(id: String, block: JobBuilder.() -> Unit) {
        val builder = JobBuilder(id).apply(block)
        when {
            builder.fixedRate != null -> scheduleAtFixedRate(id, builder.initialDelay, builder.fixedRate!!, builder.priority, builder.timeoutMs, builder.tags, builder.metadata, builder.maxRepetitions, builder.action)
            builder.fixedDelay != null -> scheduleWithFixedDelay(id, builder.initialDelay, builder.fixedDelay!!, builder.priority, builder.timeoutMs, builder.tags, builder.metadata, builder.maxRepetitions, builder.action)
            builder.atTime != null -> scheduleAt(id, builder.atTime!!, builder.priority, builder.timeoutMs, builder.tags, builder.metadata, builder.dependsOn, builder.action)
            else -> schedule(id, builder.initialDelay, builder.priority, builder.timeoutMs, builder.tags, builder.metadata, builder.dependsOn, builder.action)
        }
    }
}

class JobBuilder(val id: String) {
    var initialDelay: Long = 0
    var fixedRate: Long? = null
    var fixedDelay: Long? = null
    var atTime: Instant? = null
    var priority: Int = 0
    var timeoutMs: Long? = null
    var tags: Set<String> = emptySet()
    var metadata: Map<String, Any> = emptyMap()
    var maxRepetitions: Int? = null
    var dependsOn: String? = null
    lateinit var action: (String) -> Unit

    fun execute(block: (String) -> Unit) {
        this.action = block
    }

    fun every(ms: Long) {
        this.fixedRate = ms
    }

    fun withDelay(ms: Long) {
        this.fixedDelay = ms
    }

    fun at(time: Instant) {
        this.atTime = time
    }

    fun startAfter(ms: Long) {
        this.initialDelay = ms
    }

    fun withPriority(priority: Int) {
        this.priority = priority
    }

    fun withTimeout(ms: Long) {
        this.timeoutMs = ms
    }

    fun withTags(vararg tags: String) {
        this.tags = tags.toSet()
    }

    fun withMetadata(metadata: Map<String, Any>) {
        this.metadata = metadata
    }

    fun repeatAtMost(times: Int) {
        this.maxRepetitions = times
    }

    fun dependsOn(jobId: String) {
        this.dependsOn = jobId
    }
}

enum class JobStatus {
    RUNNING, COMPLETED, CANCELLED, NOT_FOUND
}

data class JobInfo(
    val id: String,
    val status: JobStatus,
    val executionCount: Int,
    val lastExecutionTime: Instant?,
    val intervalMs: Long?,
    val priority: Int,
    val tags: Set<String>,
    val metadata: Map<String, Any>
)

class DefaultKalanThreadFactory : ThreadFactory {
    private val counter = java.util.concurrent.atomic.AtomicInteger(0)
    override fun newThread(r: Runnable): Thread {
        return Thread(r, "kalan-scheduler-worker-${counter.getAndIncrement()}")
    }
}