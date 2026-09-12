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

    fun schedule(id: String, delayMs: Long, action: () -> Unit) {
        if (!running.get()) return
        
        val job = Job(id, action, Instant.now().plusMillis(delayMs))
        jobInstances[id] = job

        val future = scheduler.schedule({
            try {
                job.execute()
            } catch (e: Throwable) {
                errorHandler(e)
            }
        }, delayMs, TimeUnit.MILLISECONDS)
        
        activeJobs[id] = future
    }

    fun scheduleAt(id: String, startTime: Instant, action: () -> Unit) {
        val delay = Duration.between(Instant.now(), startTime).toMillis()
        schedule(id, if (delay < 0) 0 else delay, action)
    }

    fun scheduleAtFixedRate(id: String, initialDelayMs: Long, periodMs: Long, action: () -> Unit) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now().plusMillis(initialDelayMs), periodMs)
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

    fun scheduleWithFixedDelay(id: String, initialDelayMs: Long, delayMs: Long, action: () -> Unit) {
        if (!running.get()) return

        val job = Job(id, action, Instant.now().plusMillis(initialDelayMs), delayMs)
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

    fun shutdown() {
        running.set(false)
        scheduler.shutdownNow()
        jobInstances.clear()
        activeJobs.clear()
    }

    fun getActiveJobCount(): Int = activeJobs.size

    fun scheduleJob(id: String, block: JobBuilder.() -> Unit) {
        val builder = JobBuilder(id).apply(block)
        when {
            builder.fixedRate != null -> scheduleAtFixedRate(id, builder.initialDelay, builder.fixedRate!!, builder.action)
            builder.fixedDelay != null -> scheduleWithFixedDelay(id, builder.initialDelay, builder.fixedDelay!!, builder.action)
            builder.atTime != null -> scheduleAt(id, builder.atTime!!, builder.action)
            else -> schedule(id, builder.initialDelay, builder.action)
        }
    }
}

class JobBuilder(val id: String) {
    var initialDelay: Long = 0
    var fixedRate: Long? = null
    var fixedDelay: Long? = null
    var atTime: Instant? = null
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
}

enum class JobStatus {
    RUNNING, COMPLETED, CANCELLED, NOT_FOUND
}