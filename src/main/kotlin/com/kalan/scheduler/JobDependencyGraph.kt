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
            // Evaluate each branch with its own visited set so that a node skipped
            // in one branch does not incorrectly affect another branch.
            if (canReach(neighbor, target, HashSet(visited))) return true
        }
        return false
    }

    fun getDependencies(jobId: String): Set<String> =
        adj[jobId]?.toSet() ?: emptySet()

    fun getDependents(jobId: String): Set<String> =
        adj.filterValues { jobId in it }.keys.toSet()

    fun getEdgeCount(): Int = adj.values.sumOf { it.size }

    fun getAllNodes(): Set<String> = allNodes()

    private fun allNodes(): Set<String> =
        (adj.keys + adj.values.flatten()).toSet()

    fun getNodeCount(): Int = allNodes().size

    /**
     * Returns the transitive closure of dependencies for [jobId] (excluding
     * [jobId] itself). The returned set contains every node reachable by
     * following registered dependency edges.
     */
    fun getTransitiveDependencies(jobId: String): Set<String> {
        val result = mutableSetOf<String>()
        val visited = mutableSetOf(jobId)
        fun collect(node: String) {
            for (dep in getDependencies(node)) {
                if (visited.add(dep)) {
                    result.add(dep)
                    collect(dep)
                }
            }
        }
        collect(jobId)
        return result
    }

    /**
     * Returns the transitive closure of dependents for [jobId] (excluding
     * [jobId] itself).
     */
    fun getTransitiveDependents(jobId: String): Set<String> {
        val result = mutableSetOf<String>()
        val visited = mutableSetOf(jobId)
        fun collect(node: String) {
            for (dependent in getDependents(node)) {
                if (visited.add(dependent)) {
                    result.add(dependent)
                    collect(dependent)
                }
            }
        }
        collect(jobId)
        return result
    }

    /**
     * Returns the nodes that have no registered dependencies (roots).
     */
    fun getRootNodes(): Set<String> =
        allNodes().filter { getDependencies(it).isEmpty() }.toSet()

    /**
     * Returns the nodes that no other node depends on (leaves).
     */
    fun getLeafNodes(): Set<String> =
        allNodes().filter { getDependents(it).isEmpty() }.toSet()

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

        for (node in allNodes()) {
            if (depthFirstSearch(node)) return true
        }
        return false
    }

    /**
     * Returns a topological ordering of all graph nodes where dependencies come
     * before their dependents, or null when the graph contains a cycle.
     */
    fun topologicalSort(): List<String>? {
        if (hasCycle()) return null
        val nodes = allNodes()
        val inDegree = nodes.associateWith { getDependencies(it).size }.toMutableMap()
        val dependentsOf = nodes.associateWith { mutableSetOf<String>() }.toMutableMap()
        for (node in nodes) {
            for (dep in getDependencies(node)) {
                dependentsOf[dep]?.add(node)
            }
        }
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

    /**
     * Returns a deterministic summary of the dependency graph's structure.
     */
    fun getSummary(): Map<String, Any> = mapOf(
        "nodes" to getNodeCount(),
        "edges" to getEdgeCount(),
        "hasCycle" to hasCycle(),
        "rootNodes" to getRootNodes().sorted(),
        "leafNodes" to getLeafNodes().sorted(),
        "nodeIds" to allNodes().sorted(),
        "topologicalOrder" to (topologicalSort() ?: emptyList<String>()),
        "adjacency" to allNodes().sorted().associateWith { node ->
            adj[node]?.toList()?.sorted() ?: emptyList()
        }
    )

    fun clear() {
        adj.clear()
    }
}
