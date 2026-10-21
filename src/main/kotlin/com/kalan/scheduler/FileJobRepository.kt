package com.kalan.scheduler

import java.io.File
import java.util.LinkedHashMap
import java.util.concurrent.ConcurrentHashMap
import java.time.Instant

/**
 * A file-based repository.
 *
 * Job actions are lambdas and cannot be serialized directly, so this repository
 * persists the job metadata that can be reconstructed by a [JobFactory]. The
 * persisted format is `id|priority|tag1,tag2,...|metaKey=metaValue,...`.
 */
class FileJobRepository(private val storageFile: File) : JobRepository {
    private val cache = ConcurrentHashMap<String, Job>()
    private val lock = Any()

    override fun save(job: Job) = synchronized(lock) {
        cache[job.id] = job
        persist()
    }

    override fun findById(id: String): Job? = cache[id]

    override fun remove(id: String): Job? = synchronized(lock) {
        val job = cache.remove(id)
        persist()
        job
    }

    override fun findAll(): Collection<Job> = cache.values

    override fun findByTag(tag: String): List<Job> = cache.values.filter { it.tags.contains(tag) }

    override fun findByTags(tags: Set<String>): List<Job> = cache.values.filter { job -> job.tags.any { it in tags } }

    override fun findByMetadata(key: String, value: Any): List<Job> = cache.values.filter { it.metadata[key] == value }

    override fun findByPriority(priority: JobPriority): List<Job> = cache.values.filter { it.priority == priority }

    override fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job> = cache.values.filter { it.priority in min..max }

    override fun clear() = synchronized(lock) {
        cache.clear()
        if (storageFile.exists()) storageFile.delete()
    }

    private fun persist() {
        storageFile.parentFile?.mkdirs()
        if (cache.isEmpty()) {
            if (storageFile.exists()) storageFile.delete()
            return
        }
        val content = cache.values.joinToString("\n") { job ->
            val tags = job.tags.joinToString(",")
            val metadata = job.metadata.entries.joinToString(",") { "${it.key}=${it.value}" }
            "${job.id}|${job.priority.value}|$tags|$metadata"
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
     * persisted records are malformed are skipped. Any jobs previously held in
     * memory but absent from the persisted file are removed. If the file cannot
     * be read, the existing in-memory state is left untouched.
     */
    fun load(factory: JobFactory) = synchronized(lock) {
        if (!storageFile.exists()) {
            cache.clear()
            return
        }
        val lines = try {
            storageFile.readLines()
        } catch (e: Exception) {
            return
        }
        val loaded = LinkedHashMap<String, Job>()
        lines.filter { it.isNotBlank() }.forEach { line ->
            val parts = line.trim().split("|")
            if (parts.size >= 3) {
                val id = parts[0].trim()
                if (id.isEmpty()) return@forEach
                val priority = parts[1].trim().toIntOrNull() ?: JobPriority.NORMAL.value
                val tags = parseTags(parts[2].trim())
                try {
                    val job = factory.createJob(id, priority, tags)
                    if (parts.size >= 4) {
                        val metadata = parseMetadata(parts[3].trim())
                        (job.metadata as? MutableMap<String, Any>)?.putAll(metadata)
                    }
                    loaded[id] = job
                } catch (e: Exception) {
                    // Skip jobs that cannot be reconstructed.
                }
            }
        }
        cache.clear()
        cache.putAll(loaded)
    }

    private fun parseTags(raw: String): Set<String> {
        return if (raw.isEmpty()) emptySet() else raw.split(",").map { it.trim() }.filter { it.isNotEmpty() }.toSet()
    }

    private fun parseMetadata(raw: String): Map<String, String> {
        if (raw.isEmpty()) return emptyMap()
        val result = LinkedHashMap<String, String>()
        raw.split(",").forEach { entry ->
            val separator = entry.indexOf('=')
            if (separator >= 0) {
                val key = entry.substring(0, separator).trim()
                val value = entry.substring(separator + 1).trim()
                if (key.isNotEmpty()) result[key] = value
            }
        }
        return result
    }
}
