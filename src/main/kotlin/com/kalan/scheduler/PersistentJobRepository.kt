package com.kalan.scheduler

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.time.Instant

/**
 * A repository that persists job metadata to a plain text file.
 * In this lightweight version, it stores job IDs and priorities to allow
 * the scheduler to rebuild its state, though lambdas must be re-registered.
 */
class PersistentJobRepository(private val storageFile: File) : JobRepository {
    private val jobs = ConcurrentHashMap<String, Job>()

    override fun save(job: Job) {
        jobs[job.id] = job
        persist()
    }

    override fun findById(id: String): Job? = jobs[id]

    override fun remove(id: String): Job? {
        val removed = jobs.remove(id)
        persist()
        return removed
    }

    override fun findAll(): Collection<Job> = jobs.values

    override fun findByTag(tag: String): List<Job> = jobs.values.filter { it.tags.contains(tag) }

    override fun findByTags(tags: Set<String>): List<Job> = jobs.values.filter { it.tags.any { t -> t in tags } }

    override fun findByMetadata(key: String, value: Any): List<Job> = jobs.values.filter { it.metadata[key] == value }

    override fun findByPriority(priority: JobPriority): List<Job> = jobs.values.filter { it.priority == priority }

    override fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job> = jobs.values.filter { it.priority in min..max }

    override fun clear() {
        jobs.clear()
        if (storageFile.exists()) storageFile.delete()
    }

    private fun persist() {
        val content = jobs.values.joinToString("\n") {
            "${it.id}|${it.priority.value}|${it.tags.joinToString(",")}"
        }
        storageFile.writeText(content)
    }

    fun load(factory: JobFactory) {
        if (!storageFile.exists()) return
        storageFile.readLines().filter { it.isNotBlank() }.forEach {
            val parts = it.split("|")
            if (parts.size >= 3) {
                val id = parts[0]
                val priority = parts[1].toIntOrNull() ?: 10
                val tags = if (parts[2].isEmpty()) emptySet() else parts[2].split(",").toSet()
                jobs[id] = factory.createJob(id, priority, tags)
            }
        }
    }
}