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
        // Use an independent copy of the visited set for each sibling dependency so
        // that diamond-shaped dependency graphs are not incorrectly treated as blocked.
        return dependencies.all { isDependencySatisfied(it, visited.toMutableSet()) }
    }

    /**
     * Returns the dependencies of [jobId] that are not yet in a completed state,
     * including missing jobs. Useful for explaining why a job is blocked.
     */
    fun getUnsatisfiedDependencies(jobId: String): List<String> {
        val job = scheduler.getJobInfo(jobId) ?: return emptyList()
        val dependencies = linkedSetOf<String>()
        job.dependsOn?.let { dependencies.add(it) }
        dependencies.addAll(graph.getDependencies(jobId))
        return dependencies.filter { dep ->
            val depJob = scheduler.getJobInfo(dep)
            depJob == null || depJob.status != JobStatus.COMPLETED
        }.sorted()
    }

    /**
     * Registers a new dependency and validates that it doesn't create a cycle.
     * @throws IllegalArgumentException if a circular dependency is detected.
     */
    fun registerDependency(jobId: String, dependsOnId: String) {
        if (graph.wouldCreateCycle(jobId, dependsOnId)) {
            throw IllegalArgumentException("Circular dependency detected: $jobId -> $dependsOnId")
        }
        // Also guard against cycles introduced through job-level dependsOn links.
        if (wouldCreateCombinedCycle(jobId, dependsOnId)) {
            throw IllegalArgumentException("Circular dependency detected: $jobId -> $dependsOnId")
        }
        graph.addDependency(jobId, dependsOnId)
    }

    private fun wouldCreateCombinedCycle(jobId: String, dependsOnId: String): Boolean {
        if (jobId == dependsOnId) return true
        // Treat the proposed edge as present and check reachability.
        val visited = mutableSetOf<String>()
        fun canReach(start: String, target: String): Boolean {
            if (start == target) return true
            if (!visited.add(start)) return false
            val neighbors = combinedDependencies(start).toMutableSet()
            if (start == jobId) neighbors.add(dependsOnId)
            for (neighbor in neighbors) {
                if (canReach(neighbor, target)) return true
            }
            return false
        }
        return canReach(dependsOnId, jobId)
    }

    private fun combinedDependencies(node: String): Set<String> {
        val result = linkedSetOf<String>()
        scheduler.getJobInfo(node)?.dependsOn?.let { result.add(it) }
        result.addAll(graph.getDependencies(node))
        return result
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
     * Returns direct dependents of [jobId], considering both registered graph
     * edges and job-level [Job.dependsOn] links.
     */
    fun getDirectDependents(jobId: String): Set<String> {
        val result = mutableSetOf<String>()
        result.addAll(graph.getDependents(jobId))
        scheduler.listAllJobs().forEach { job ->
            if (job.dependsOn == jobId) result.add(job.id)
        }
        return result
    }

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

    /**
     * Returns all transitive dependencies of [jobId], following both job-level
     * [Job.dependsOn] links and registered graph edges.
     */
    fun getTransitiveDependencies(jobId: String): Set<String> {
        val result = mutableSetOf<String>()
        val visited = mutableSetOf(jobId)
        val queue = ArrayDeque<String>()
        val seed = linkedSetOf<String>()
        scheduler.getJobInfo(jobId)?.dependsOn?.let { seed.add(it) }
        seed.addAll(graph.getDependencies(jobId))
        queue.addAll(seed)
        while (queue.isNotEmpty()) {
            val node = queue.removeFirst()
            if (!visited.add(node)) continue
            result.add(node)
            val next = linkedSetOf<String>()
            scheduler.getJobInfo(node)?.dependsOn?.let { next.add(it) }
            next.addAll(graph.getDependencies(node))
            next.forEach { if (it !in visited) queue.add(it) }
        }
        return result
    }

    /**
     * Returns all transitive dependents of [jobId], following both job-level
     * [Job.dependsOn] links and registered graph edges.
     */
    fun getTransitiveDependents(jobId: String): Set<String> {
        val result = mutableSetOf<String>()
        val visited = mutableSetOf(jobId)
        val queue = ArrayDeque<String>()
        queue.add(jobId)
        while (queue.isNotEmpty()) {
            val node = queue.removeFirst()
            val dependents = getDirectDependents(node)
            for (dependent in dependents) {
                if (visited.add(dependent)) {
                    result.add(dependent)
                    queue.add(dependent)
                }
            }
        }
        return result
    }

    /**
     * Returns true when the combined dependency graph (job-level dependsOn plus
     * registered edges) contains at least one cycle.
     */
    fun hasCycle(): Boolean = hasAnyNode() && getTopologicalOrder() == null

    private fun hasAnyNode(): Boolean =
        scheduler.listAllJobs().isNotEmpty() || graph.getAllNodes().isNotEmpty()

    /**
     * Returns a topological ordering of all known jobs where dependencies come
     * before their dependents, or null when a cycle exists.
     */
    fun getTopologicalOrder(): List<String>? {
        val nodes = (scheduler.listAllJobs().map { it.id } + graph.getAllNodes()).toSet()
        if (nodes.isEmpty()) return emptyList()
        val depsOf = LinkedHashMap<String, Set<String>>()
        for (node in nodes) {
            val direct = linkedSetOf<String>()
            scheduler.getJobInfo(node)?.dependsOn?.let { if (it in nodes) direct.add(it) }
            direct.addAll(graph.getDependencies(node).filter { it in nodes })
            depsOf[node] = direct
        }
        val dependentsOf = nodes.associateWith { mutableSetOf<String>() }.toMutableMap()
        for (node in nodes) {
            for (dep in depsOf[node]!!) {
                dependentsOf[dep]?.add(node)
            }
        }
        val inDegree = nodes.associateWith { depsOf[it]!!.size }.toMutableMap()
        val queue = ArrayDeque(nodes.filter { inDegree[it] == 0 }.sorted())
        val result = mutableListOf<String>()
        while (queue.isNotEmpty()) {
            val node = queue.removeFirst()
            result.add(node)
            for (dependent in dependentsOf[node]?.sorted() ?: emptyList()) {
                inDegree[dependent] = inDegree[dependent]!! - 1
                if (inDegree[dependent] == 0) queue.add(dependent)
            }
        }
        return if (result.size == nodes.size) result else null
    }

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
                "unsatisfiedDependencies" to emptyList<String>(),
                "unsatisfiedDependencyCount" to 0,
                "transitiveDependencies" to emptyList<String>(),
                "transitiveDependents" to emptyList<String>(),
                "dependencyChain" to emptyList<String>(),
                "dependencyDepth" to 0,
                "dependencyStatus" to "NONE",
                "hasCycle" to hasCycle(),
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
            "unsatisfiedDependencies" to getUnsatisfiedDependencies(jobId),
            "unsatisfiedDependencyCount" to getUnsatisfiedDependencies(jobId).size,
            "transitiveDependencies" to getTransitiveDependencies(jobId).sorted(),
            "transitiveDependents" to getTransitiveDependents(jobId).sorted(),
            "dependencyChain" to chain,
            "dependencyDepth" to (chain.size - 1).coerceAtLeast(0),
            "dependencyStatus" to dependencyStatus,
            "hasCycle" to hasCycle(),
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
        val leafJobs = jobs.filter { getDirectDependents(it.id).isEmpty() }
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
            "leafJobs" to leafJobs.size,
            "leafJobIds" to leafJobs.map { it.id }.sorted(),
            "readyJobs" to readyJobs.size,
            "readyJobIds" to readyJobs.sorted(),
            "blockedJobs" to blockedJobs.size,
            "blockedJobIds" to blockedJobs.sorted(),
            "maxDependencyDepth" to maxDepth,
            "hasCycle" to hasCycle(),
            "topologicalOrder" to (getTopologicalOrder() ?: emptyList<String>()),
            "graphSummary" to graph.getSummary()
        )
    }

    fun getSummary(): Map<String, Any> = getDependencySummary()

    fun getJobSummary(jobId: String): Map<String, Any> = getJobDependencySummary(jobId)
}
