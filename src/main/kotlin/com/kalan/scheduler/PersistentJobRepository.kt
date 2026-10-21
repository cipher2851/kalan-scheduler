package com.kalan.scheduler

import java.io.File
import java.util.LinkedHashMap
import java.util.concurrent.ConcurrentHashMap
import java.time.Instant

/**
 * A repository that persists job metadata to a plain text file.
 * In this lightweight version, it stores job IDs, priorities, tags and metadata
 * to allow the scheduler to rebuild its state, though lambdas must be re-registered.
 */
class PersistentJobRepository(private val storageFile: File) : JobRepository {
    private val jobs = ConcurrentHashMap<String, Job>()
    private val lock = Any()

    override fun save(job: Job) = synchronized(lock) {
        jobs[job.id] = job
        persist()
    }

    override fun findById(id: String): Job? = jobs[id]

    override fun remove(id: String): Job? = synchronized(lock) {
        val removed = jobs.remove(id)
        persist()
        removed
    }

    override fun findAll(): Collection<Job> = jobs.values

    override fun findByTag(tag: String): List<Job> = jobs.values.filter { it.tags.contains(tag) }

    override fun findByTags(tags: Set<String>): List<Job> = jobs.values.filter { it.tags.any { t -> t in tags } }

    override fun findByMetadata(key: String, value: Any): List<Job> = jobs.values.filter { it.metadata[key] == value }

    override fun findByPriority(priority: JobPriority): List<Job> = jobs.values.filter { it.priority == priority }

    override fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job> = jobs.values.filter { it.priority in min..max }

    override fun clear() = synchronized(lock) {
        jobs.clear()
        if (storageFile.exists()) storageFile.delete()
    }

    private fun persist() {
        storageFile.parentFile?.mkdirs()
        if (jobs.isEmpty()) {
            if (storageFile.exists()) storageFile.delete()
            return
        }
        val content = jobs.values.joinToString("\n") { job ->
            val tags = job.tags.joinToString(",")
            val metadata = job.metadata.entries.joinToString(",") { "${it.key}=${it.value}" }
            "${job.id}|${job.priority.value}|$tags|$metadata"
        }
        storageFile.writeText(content)
    }

    fun load(factory: JobFactory) = synchronized(lock) {
        if (!storageFile.exists()) {
            jobs.clear()
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
                val tags = if (parts[2].trim().isEmpty()) emptySet() else parts[2].trim().split(",").map { it.trim() }.filter { it.isNotEmpty() }.toSet()
                try {
                    val job = factory.createJob(id, priority, tags)
                    if (parts.size >= 4 && parts[3].trim().isNotEmpty()) {
                        val metadata = LinkedHashMap<String, String>()
                        parts[3].trim().split(",").forEach { entry ->
                            val separator = entry.indexOf('=')
                            if (separator >= 0) {
                                val key = entry.substring(0, separator).trim()
                                val value = entry.substring(separator + 1).trim()
                                if (key.isNotEmpty()) metadata[key] = value
                            }
                        }
                        (job.metadata as? MutableMap<String, Any>)?.putAll(metadata)
                    }
                    loaded[id] = job
                } catch (e: Exception) {
                    // Skip jobs that cannot be reconstructed.
                }
            }
        }
        jobs.clear()
        jobs.putAll(loaded)
    }
}
