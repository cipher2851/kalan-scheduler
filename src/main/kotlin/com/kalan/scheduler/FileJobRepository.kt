package com.kalan.scheduler

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.time.Instant

/**
 * A simple File-based repository. 
 * Note: In a real-world scenario, this would use a JSON library like Jackson or kotlinx.serialization.
 * For this lightweight implementation, we store basic job metadata as properties.
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

    override fun findByMetadata(key: String, value: Any): List<Job> = cache.values.filter { it.metadata[key] == value }

    override fun clear() {
        cache.clear()
        if (storageFile.exists()) storageFile.delete()
    }

    private fun persist() {
        // Minimalist persistence: only IDs and basic config are stored here
        // In a full impl, we would serialize the Job objects to JSON
        storageFile.writeText(cache.keys.joinToString("\n"))
    }

    fun load() {
        if (!storageFile.exists()) return
        val ids = storageFile.readText().lines().filter { it.isNotBlank() }
        // Note: Actual Job actions cannot be persisted to disk as they are lambdas.
        // A real persistence layer would require Jobs to be defined by class names/factories.
    }
}