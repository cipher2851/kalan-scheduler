package com.kalan.scheduler

import java.time.Duration
import java.time.Instant
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

class KalanScheduler(corePoolSize: Int = 1) {
    private val scheduler = ScheduledThreadPoolExecutor(corePoolSize)
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

    fun schedule(id: String, delayMs: Long, priority: Int = 0, timeoutMs: Long? = null, action: () -> Unit) {
        if (!running.get()) return
        
        val job = Job(id, action, Instant.now().plusMillis(delayMs), priority = priority, timeoutMs = timeoutMs)
        jobInstances[id] = job

        val future = scheduler.schedule({
            try {
                job.execute()
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

    fun scheduleAt(id: String, startTime: Instant, priority: Int = 0, timeoutMs: Long? = null, action: () -> Unit) {
        val delay = Duration.between(Instant.now(), startTime).toMillis()
        schedule(id, if (delay < 0) 0 else delay, priority, timeoutMs, action)
    }

    fun scheduleAtFixedRate(id: String, initialDelayMs: Long, periodMs: Long, priority: Int = 0, timeoutMs: Long? = null, action: () -> Unit) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now().plusMillis(initialDelayMs), periodMs, priority, timeoutMs)
        jobInstances[id] = job

        val future = scheduler.scheduleAtFixedRate({
            try {
                job.execute()
            } catch (e: Throwable) {
                errorHandler(e)
            }
        }, initialDelayMs, periodMs, TimeUnit.MILLISECONDS)

        activeJobs[id] = future
    }

    fun scheduleWithFixedDelay(id: String, initialDelayMs: Long, delayMs: Long, priority: Int = 0, timeoutMs: Long? = null, action: () -> Unit) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now().plusMillis(initialDelayMs), delayMs, priority, timeoutMs)
        jobInstances[id] = job

        val future = scheduler.scheduleWithFixedDelay({
            try {
                job.execute()
            } catch (e: Throwable) {
                errorHandler(e)
            }
        }, initialDelayMs, delayMs, TimeUnit.MILLISECONDS)

        activeJobs[id] = future
    }

    fun updateJob(id: String, newAction: () -> Unit) {
        jobInstances[id]?.action = newAction
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
            JobInfo(id, getJobStatus(id), it.getExecutionCount(), it.getLastExecutionTime(), it.intervalMs, it.priority)
        }

    fun listAllJobs(): List<JobInfo> {
        return jobInstances.keys.mapNotNull { getJobInfo(it) }
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
            builder.fixedRate != null -> scheduleAtFixedRate(id, builder.initialDelay, builder.fixedRate!!, builder.priority, builder.timeoutMs, builder.action)
            builder.fixedDelay != null -> scheduleWithFixedDelay(id, builder.initialDelay, builder.fixedDelay!!, builder.priority, builder.timeoutMs, builder.action)
            builder.atTime != null -> scheduleAt(id, builder.atTime!!, builder.priority, builder.timeoutMs, builder.action)
            else -> schedule(id, builder.initialDelay, builder.priority, builder.timeoutMs, builder.action)
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
    lateinit var action: () -> Unit

    fun execute(block: () -> Unit) {
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
    val priority: Int
)