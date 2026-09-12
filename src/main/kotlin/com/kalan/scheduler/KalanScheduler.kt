package com.kalan.scheduler

import java.time.Instant
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

class KalanScheduler(corePoolSize: Int = 1) {
    private val scheduler = ScheduledThreadPoolExecutor(corePoolSize)
    private val activeJobs = ConcurrentHashMap<String, ScheduledFuture<*>>()
    private val running = AtomicBoolean(true)

    fun schedule(id: String, delayMs: Long, action: () -> Unit) {
        if (!running.get()) return
        
        val future = scheduler.schedule({
            try {
                action()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }, delayMs, TimeUnit.MILLISECONDS)
        
        activeJobs[id] = future
    }

    fun scheduleAtFixedRate(id: String, initialDelayMs: Long, periodMs: Long, action: () -> Unit) {
        if (!running.get()) return

        val future = scheduler.scheduleAtFixedRate({
            try {
                action()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }, initialDelayMs, periodMs, TimeUnit.MILLISECONDS)

        activeJobs[id] = future
    }

    fun cancel(id: String) {
        activeJobs.remove(id)?.cancel(false)
    }

    fun isJobActive(id: String): Boolean {
        val future = activeJobs[id]
        return future != null && !future.isDone
    }

    fun shutdown() {
        running.set(false)
        scheduler.shutdownNow()
    }

    fun getActiveJobCount(): Int = activeJobs.size
}