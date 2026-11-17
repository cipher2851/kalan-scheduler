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

    fun getDependencies(jobId: String): Set<String> =
        adj[jobId]?.toSet() ?: emptySet()

    fun getDependents(jobId: String): Set<String> =
        adj.filterValues { jobId in it }.keys.toSet()

    fun getEdgeCount(): Int = adj.values.sumOf { it.size }

    private fun allNodes(): Set<String> =
        (adj.keys + adj.values.flatten()).toSet()

    fun getNodeCount(): Int = allNodes().size

    /**
     * Returns true when the dependency graph currently contains at least one cycle.
     */
    fun hasCycle(): Boolean {
        val visited = mutableSetOf<String>()
        val recursionStack = mutableSetOf<String>()

        fun depthFirstSearch(node: String): Boolean {
            if (node in recursionStack) return true
            if (node in visited) return false
            visited.add(node)
            recursionStack.add(node)
            for (neighbor in adj[node] ?: emptySet<String>()) {
                if (depthFirstSearch(neighbor)) return true
            }
            recursionStack.remove(node)
            return false
        }

        for (node in adj.keys.toList()) {
            if (depthFirstSearch(node)) return true
        }
        return false
    }

    /**
     * Returns a deterministic summary of the dependency graph's structure.
     */
    fun getSummary(): Map<String, Any> = mapOf(
        "nodes" to getNodeCount(),
        "edges" to getEdgeCount(),
        "hasCycle" to hasCycle(),
        "nodeIds" to allNodes().sorted(),
        "adjacency" to allNodes().sorted().associateWith { node ->
            adj[node]?.toList()?.sorted() ?: emptyList()
        }
    )

    fun clear() {
        adj.clear()
    }
}
