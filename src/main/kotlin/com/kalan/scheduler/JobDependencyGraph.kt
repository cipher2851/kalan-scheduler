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

    fun wouldCreateCycle(jobId: String, dependsOnId: String): Boolean {
        val visited = mutableSetOf<String>()
        val stack = mutableSetOf<String>()

        fun hasCycle(u: String): Boolean {
            visited.add(u)
            stack.add(u)

            val neighbors = adj[u] ?: emptySet<String>()
            for (v in neighbors) {
                if (v == jobId) return true
                if (v !in visited) {
                    if (hasCycle(v)) return true
                }
            }

            stack.remove(u)
            return false
        }

        // Check if adding (jobId -> dependsOnId) creates a cycle
        // We simulate the edge by starting a DFS from dependsOnId to see if we can reach jobId
        return canReach(dependsOnId, jobId, mutableSetOf())
    }

    private fun canReach(start: String, target: String, visited: MutableSet<String>): Boolean {
        if (start == target) return true
        visited.add(start)
        val neighbors = adj[start] ?: emptySet<String>()
        for (neighbor in neighbors) {
            if (neighbor !in visited) {
                if (canReach(neighbor, target, visited)) return true
            }
        }
        return false
    }

    fun clear() {
        adj.clear()
    }
}
