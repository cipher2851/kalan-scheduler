package com.kalan.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger

/**
 * Actively monitors the health of jobs and triggers callbacks when a job becomes unhealthy.
 */
class JobHealthCheckManager(
    private val scheduler: KalanScheduler,
    private val checkIntervalMs: Long = 60000,
    private val failureThreshold: Double = 0.5,
    private val minAttemptsForHealthCheck: Int = 5
) {
    private val logger = Logger.getLogger(JobHealthCheckManager::class.java.name)
    private val isRunning = AtomicBoolean(false)
    private val healthListeners = java.util.concurrent.CopyOnWriteArrayList<JobHealthListener>()
    private val unhealthyJobs = ConcurrentHashMap.newKeySet<String>()

    fun addListener(listener: JobHealthListener) {
        healthListeners.add(listener)
    }

    fun removeListener(listener: JobHealthListener) {
        healthListeners.remove(listener)
    }

    fun start() {
        if (isRunning.getAndSet(true)) return

        try {
            scheduler.scheduleAtFixedRate("kalan-internal-health-check", 0, checkIntervalMs) {
                checkAllJobsHealth()
                null
            }
        } catch (e: Exception) {
            isRunning.set(false)
            throw e
        }
    }

    fun stop() {
        if (!isRunning.getAndSet(false)) return

        try {
            scheduler.cancel("kalan-internal-health-check")
        } catch (e: Exception) {
            logger.warning("Failed to cancel health check job: ${e.message}")
        }
        unhealthyJobs.clear()
    }

    private fun checkAllJobsHealth() {
        val jobs = scheduler.listAllJobs()
        jobs.forEach {
            val stats = it.getStatistics()
            val total = stats.totalAttempts
            val failureRate = stats.failureRate

            if (total >= minAttemptsForHealthCheck && failureRate > failureThreshold) {
                if (unhealthyJobs.add(it.id)) {
                    notifyHealthChange(it, JobHealth.UNHEALTHY)
                }
            } else if (unhealthyJobs.contains(it.id)) {
                // Recovered
                unhealthyJobs.remove(it.id)
                notifyHealthChange(it, JobHealth.HEALTHY)
            }
        }
    }

    private fun notifyHealthChange(job: Job, health: JobHealth) {
        logger.warning("Job ${job.id} health changed to $health")
        healthListeners.forEach { it.onHealthChange(job.id, health) }
    }

    fun getUnhealthyJobs(): Set<String> = unhealthyJobs.toSet()
}

/**
 * Listener for job health changes.
 */
fun interface JobHealthListener {
    fun onHealthChange(jobId: String, health: JobHealth)
}
