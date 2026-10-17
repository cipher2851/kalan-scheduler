package com.kalan.scheduler

import java.util.concurrent.ConcurrentHashMap

/**
 * Manages the execution readiness of jobs based on their dependencies.
 * It ensures that a job is only eligible for execution if all its dependencies
 * have reached a terminal successful state.
 */
class JobDependencyManager(
    private val graph: JobDependencyGraph,
    private val scheduler: KalanScheduler
) {
    private val dependencyStates = ConcurrentHashMap<String, Boolean>()

    /**
     * Checks if a job is ready to be executed based on its defined dependencies.
     */
    fun isReady(jobId: String): Boolean {
        val job = scheduler.getJobInfo(jobId) ?: return false
        val dependsOn = job.dependsOn ?: return true

        // A job is ready if its dependency has completed successfully
        return isDependencySatisfied(dependsOn)
    }

    private fun isDependencySatisfied(dependencyId: String): Boolean {
        val depJob = scheduler.getJobInfo(dependencyId) ?: return false
        
        // We consider a dependency satisfied if it's COMPLETED. 
        // In a more complex system, we might allow satisfied on certain failures.
        return depJob.status == JobStatus.COMPLETED
    }

    /**
     * Registers a new dependency and validates that it doesn't create a cycle.
     * @throws IllegalArgumentException if a circular dependency is detected.
     */
    fun registerDependency(jobId: String, dependsOnId: String) {
        if (graph.wouldCreateCycle(jobId, dependsOnId)) {
            throw IllegalArgumentException("Circular dependency detected: $jobId -> $dependsOnId")
        }
        graph.addDependency(jobId, dependsOnId)
    }

    fun removeJob(jobId: String) {
        graph.removeJob(jobId)
        dependencyStates.remove(jobId)
    }

    fun clear() {
        graph.clear()
        dependencyStates.clear()
    }
}
