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

    fun getHealthStatus(jobId: String): JobHealth =
        if (unhealthyJobs.contains(jobId)) JobHealth.UNHEALTHY else JobHealth.HEALTHY

    fun isJobHealthy(jobId: String): Boolean = getHealthStatus(jobId) == JobHealth.HEALTHY

    fun getHealthyJobs(): Set<String> {
        val allJobIds = scheduler.listAllJobs().map { it.id }.toSet()
        return allJobIds.filter { !unhealthyJobs.contains(it) }.toSet()
    }

    fun getHealthSummary(): Map<String, Any> {
        val jobs = scheduler.listAllJobs()
        val unhealthy = getUnhealthyJobs()
        val healthy = jobs.size - unhealthy.size
        return mapOf(
            "totalJobs" to jobs.size,
            "healthyJobs" to healthy.coerceAtLeast(0),
            "unhealthyJobs" to unhealthy.size,
            "healthyJobIds" to getHealthyJobs().sorted(),
            "unhealthyJobIds" to unhealthy.sorted(),
            "healthRate" to if (jobs.isEmpty()) 0.0 else healthy.toDouble() / jobs.size,
            "failureThreshold" to failureThreshold,
            "minAttemptsForHealthCheck" to minAttemptsForHealthCheck,
            "checkIntervalMs" to checkIntervalMs,
            "isRunning" to isRunning.get()
        )
    }

    fun getUnhealthyJobs(): Set<String> = unhealthyJobs.toSet()

    /**
     * Returns a detailed health summary for a single job. Jobs that have not been
     * registered with the scheduler are reported as absent.
     */
    fun getJobHealthSummary(jobId: String): Map<String, Any> {
        val job = scheduler.getJobInfo(jobId)
        val health = getHealthStatus(jobId)
        val isHealthy = health == JobHealth.HEALTHY
        if (job == null) {
            return mapOf(
                "jobId" to jobId,
                "exists" to false,
                "health" to health,
                "healthStatus" to health,
                "isHealthy" to isHealthy,
                "healthy" to isHealthy
            )
        }
        val stats = job.getStatistics()
        return mapOf(
            "jobId" to jobId,
            "exists" to true,
            "health" to health,
            "healthStatus" to health,
            "isHealthy" to isHealthy,
            "healthy" to isHealthy,
            "jobStatus" to job.getJobStatus(),
            "totalAttempts" to stats.totalAttempts,
            "failureRate" to stats.failureRate,
            "failureThreshold" to failureThreshold,
            "minAttemptsForHealthCheck" to minAttemptsForHealthCheck
        )
    }

    /**
     * Returns detailed health summaries for all jobs currently known to the
     * scheduler.
     */
    fun getHealthSummaries(): List<Map<String, Any>> =
        scheduler.listAllJobs().map { getJobHealthSummary(it.id) }

    /**
     * Alias for [getJobHealthSummary], matching the aggregate naming style used
     * by other summary APIs.
     */
    fun getHealthSummary(jobId: String): Map<String, Any> = getJobHealthSummary(jobId)
}

/**
 * Listener for job health changes.
 */
fun interface JobHealthListener {
    fun onHealthChange(jobId: String, health: JobHealth)
}
