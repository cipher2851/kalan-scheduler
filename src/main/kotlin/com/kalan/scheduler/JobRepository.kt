package com.kalan.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CompletableFuture

interface JobRepository {
    fun save(job: Job)
    fun findById(id: String): Job?
    fun remove(id: String): Job?
    fun findAll(): Collection<Job>
    fun findByTag(tag: String): List<Job>
    fun findByMetadata(key: String, value: Any): List<Job>
    fun clear()
}

/**
 * A wrapper that provides asynchronous access to a JobRepository.
 */
class AsyncJobRepository(private val delegate: JobRepository) : JobRepository {
    private val executor = java.util.concurrent.Executors.newSingleThreadExecutor()

    fun saveAsync(job: Job): CompletableFuture<Void> = CompletableFuture.runAsync({
        delegate.save(job)
    }, executor)

    fun findByIdAsync(id: String): CompletableFuture<Job?> = CompletableFuture.supplyAsync({
        delegate.findById(id)
    }, executor)

    fun removeAsync(id: String): CompletableFuture<Job?> = CompletableFuture.supplyAsync({
        delegate.remove(id)
    }, executor)

    fun findAllAsync(): CompletableFuture<Collection<Job>> = CompletableFuture.supplyAsync({
        delegate.findAll()
    }, executor)

    override fun save(job: Job) = delegate.save(job)
    override fun findById(id: String): Job? = delegate.findById(id)
    override fun remove(id: String): Job? = delegate.remove(id)
    override fun findAll(): Collection<Job> = delegate.findAll()
    override fun findByTag(tag: String): List<Job> = delegate.findByTag(tag)
    override fun findByMetadata(key: String, value: Any): List<Job> = delegate.findByMetadata(key, value)
    override fun clear() = delegate.clear()

    fun shutdown() {
        executor.shutdown()
    }
}

class InMemoryJobRepository : JobRepository {
    private val jobs = ConcurrentHashMap<String, Job>()

    override fun save(job: Job) {
        jobs[job.id] = job
    }

    override fun findById(id: String): Job? = jobs[id]

    override fun remove(id: String): Job? = jobs.remove(id)

    override fun findAll(): Collection<Job> = jobs.values

    override fun findByTag(tag: String): List<Job> {
        return jobs.values.filter { it.tags.contains(tag) }
    }

    override fun findByMetadata(key: String, value: Any): List<Job> {
        return jobs.values.filter { it.metadata[key] == value }
    }

    override fun clear() {
        jobs.clear()
    }
}

/**
 * A simple Map-based implementation of JobRepository.
 */
class MapJobRepository(private val storage: MutableMap<String, Job> = ConcurrentHashMap()) : JobRepository {
    override fun save(job: Job) { storage[job.id] = job }
    override fun findById(id: String): Job? = storage[id]
    override fun remove(id: String): Job? = storage.remove(id)
    override fun findAll(): Collection<Job> = storage.values
    override fun findByTag(tag: String): List<Job> = storage.values.filter { it.tags.contains(tag) }
    override fun findByMetadata(key: String, value: Any): List<Job> = storage.values.filter { it.metadata[key] == value }
    override fun clear() = storage.clear()
}