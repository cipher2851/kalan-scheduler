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
        val dependencies = linkedSetOf<String>()
        job.dependsOn?.let { dependencies.add(it) }
        dependencies.addAll(graph.getDependencies(jobId))

        if (dependencies.isEmpty()) return true
        return dependencies.all { isDependencySatisfied(it, mutableSetOf()) }
    }

    private fun isDependencySatisfied(dependencyId: String, visited: MutableSet<String>): Boolean {
        if (!visited.add(dependencyId)) return false
        val depJob = scheduler.getJobInfo(dependencyId) ?: return false
        if (depJob.status != JobStatus.COMPLETED) return false

        val dependencies = linkedSetOf<String>()
        depJob.dependsOn?.let { dependencies.add(it) }
        dependencies.addAll(graph.getDependencies(dependencyId))
        return dependencies.all { isDependencySatisfied(it, visited) }
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

    fun getDependencies(jobId: String): Set<String> = graph.getDependencies(jobId)

    fun getDependents(jobId: String): Set<String> = graph.getDependents(jobId)

    /**
     * Returns the ordered chain of jobs from [jobId] through its transitive
     * dependencies. The first element is [jobId] itself.
     */
    fun getDependencyChain(jobId: String): List<String> {
        val chain = mutableListOf<String>()
        val seen = mutableSetOf<String>()
        var current: String? = jobId
        while (current != null && seen.add(current)) {
            chain.add(current)
            current = scheduler.getJobInfo(current)?.dependsOn
        }
        return chain
    }

    fun getDependencyDepth(jobId: String): Int =
        (getDependencyChain(jobId).size - 1).coerceAtLeast(0)

    fun getReadyJobs(): List<String> =
        scheduler.listAllJobs().map { it.id }.filter { isReady(it) }.sorted()

    fun getBlockedJobs(): List<String> =
        scheduler.listAllJobs().map { it.id }.filter { !isReady(it) }.sorted()

    /**
     * Returns a detailed summary for a single job's dependency state.
     */
    fun getJobDependencySummary(jobId: String): Map<String, Any> {
        val job = scheduler.getJobInfo(jobId)
        if (job == null) {
            return mapOf(
                "jobId" to jobId,
                "exists" to false,
                "isReady" to false,
                "ready" to false,
                "dependsOn" to "NONE",
                "dependencies" to emptyList<String>(),
                "dependencyCount" to 0,
                "dependencyChain" to emptyList<String>(),
                "dependencyDepth" to 0,
                "dependencyStatus" to "NONE",
                "hasCycle" to graph.hasCycle(),
                "dependents" to emptyList<String>()
            )
        }
        val dependsOn = job.dependsOn
        val dependencies = (listOfNotNull(dependsOn) + graph.getDependencies(jobId).toList()).distinct().sorted()
        val chain = getDependencyChain(jobId)
        val ready = isReady(jobId)
        val dependencyStatus = dependsOn?.let { scheduler.getJobInfo(it)?.status?.name } ?: "NONE"
        return mapOf(
            "jobId" to jobId,
            "exists" to true,
            "dependsOn" to (dependsOn ?: "NONE"),
            "dependencies" to dependencies,
            "dependencyCount" to dependencies.size,
            "isReady" to ready,
            "ready" to ready,
            "dependencyChain" to chain,
            "dependencyDepth" to (chain.size - 1).coerceAtLeast(0),
            "dependencyStatus" to dependencyStatus,
            "hasCycle" to graph.hasCycle(),
            "dependents" to getDependents(jobId).sorted()
        )
    }

    /**
     * Returns an aggregate summary of dependency readiness across all jobs.
     */
    fun getDependencySummary(): Map<String, Any> {
        val jobs = scheduler.listAllJobs()
        val jobIds = jobs.map { it.id }
        val jobsWithDependencies = jobs.filter { it.dependsOn != null || graph.getDependencies(it.id).isNotEmpty() }
        val rootJobs = jobs.filter { it.dependsOn == null && graph.getDependencies(it.id).isEmpty() }
        val readyJobs = jobIds.filter { isReady(it) }
        val blockedJobs = jobIds.filter { !isReady(it) }
        val maxDepth = jobs.map { getDependencyDepth(it.id) }.maxOrNull() ?: 0
        return mapOf(
            "totalJobs" to jobs.size,
            "total" to jobs.size,
            "jobsWithDependencies" to jobsWithDependencies.size,
            "dependencyEdges" to graph.getEdgeCount(),
            "rootJobs" to rootJobs.size,
            "rootJobIds" to rootJobs.map { it.id }.sorted(),
            "readyJobs" to readyJobs.size,
            "readyJobIds" to readyJobs.sorted(),
            "blockedJobs" to blockedJobs.size,
            "blockedJobIds" to blockedJobs.sorted(),
            "maxDependencyDepth" to maxDepth,
            "hasCycle" to graph.hasCycle(),
            "graphSummary" to graph.getSummary()
        )
    }

    fun getSummary(): Map<String, Any> = getDependencySummary()

    fun getJobSummary(jobId: String): Map<String, Any> = getJobDependencySummary(jobId)
}
