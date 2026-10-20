package com.kalan.scheduler

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.time.Instant

/**
 * A file-based repository.
 *
 * Job actions are lambdas and cannot be serialized directly, so this repository
 * persists the job metadata that can be reconstructed by a [JobFactory]. The
 * persisted format is `id|priority|tag1,tag2,...`.
 */
class FileJobRepository(private val storageFile: File) : JobRepository {
    private val cache = ConcurrentHashMap<String, Job>()

    override fun save(job: Job) {
        cache[job.id] = job
        persist()
    }

    override fun findById(id: String): Job? = cache[id]

    override fun remove(id: String): Job? {
        val job = cache.remove(id)
        persist()
        return job
    }

    override fun findAll(): Collection<Job> = cache.values

    override fun findByTag(tag: String): List<Job> = cache.values.filter { it.tags.contains(tag) }

    override fun findByTags(tags: Set<String>): List<Job> = cache.values.filter { job -> job.tags.any { it in tags } }

    override fun findByMetadata(key: String, value: Any): List<Job> = cache.values.filter { it.metadata[key] == value }

    override fun findByPriority(priority: JobPriority): List<Job> = cache.values.filter { it.priority == priority }

    override fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job> = cache.values.filter { it.priority in min..max }

    override fun clear() {
        cache.clear()
        if (storageFile.exists()) storageFile.delete()
    }

    private fun persist() {
        val content = cache.values.joinToString("\n") {
            "${it.id}|${it.priority.value}|${it.tags.joinToString(",")}"
        }
        storageFile.writeText(content)
    }

    /**
     * Backwards-compatible no-op. Job actions cannot be reconstructed without a
     * factory, so callers that need recovery should use [load] with a factory.
     */
    fun load() {
        // Retained for compatibility with earlier versions of this repository.
    }

    /**
     * Restores persisted job metadata using the supplied factory. Jobs whose
     * persisted records are malformed are skipped.
     */
    fun load(factory: JobFactory) {
        if (!storageFile.exists()) return
        storageFile.readLines().filter { it.isNotBlank() }.forEach { line ->
            val parts = line.split("|")
            if (parts.size >= 3) {
                val id = parts[0]
                val priority = parts[1].toIntOrNull() ?: JobPriority.NORMAL.value
                val tags = if (parts[2].isEmpty()) emptySet() else parts[2].split(",").toSet()
                cache[id] = factory.createJob(id, priority, tags)
            }
        }
    }
}
