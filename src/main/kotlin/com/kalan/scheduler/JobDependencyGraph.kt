package com.kalan.scheduler

import java.util.concurrent.ConcurrentHashMap

/**
 * Manages the dependency relationships between jobs to ensure no circular dependencies exist.
 */
class JobDependencyGraph {
    private val adj = ConcurrentHashMap<String, MutableSet<String>>()

    fun addDependency(jobId: String, dependsOnId: String?) {
        if (dependsOnId == null) return
        adj.computeIfAbsent(jobId) { ConcurrentHashMap.newKeySet() }.add(dependsOnId)
    }

    fun removeJob(jobId: String) {
        adj.remove(jobId)
        adj.values.forEach { it.remove(jobId) }
    }

    /**
     * Returns true if adding an edge `jobId -> dependsOnId` would create a cycle.
     */
    fun wouldCreateCycle(jobId: String, dependsOnId: String): Boolean {
        if (jobId == dependsOnId) return true
        // Simulate the proposed edge by checking whether dependsOnId can already
        // reach jobId in the existing graph.
        return canReach(dependsOnId, jobId, mutableSetOf())
    }

    private fun canReach(start: String, target: String, visited: MutableSet<String>): Boolean {
        if (start == target) return true
        if (!visited.add(start)) return false
        val neighbors = adj[start] ?: emptySet<String>()
        for (neighbor in neighbors) {
            if (canReach(neighbor, target, visited)) return true
        }
        return false
    }

    fun clear() {
        adj.clear()
    }
}
