package com.kalan.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentSkipListSet

interface JobRepository {
    fun save(job: Job)
    fun findById(id: String): Job?
    fun remove(id: String): Job?
    fun findAll(): Collection<Job>
    fun findByTag(tag: String): List<Job>
    fun findByTags(tags: Set<String>): List<Job>
    fun findByMetadata(key: String, value: Any): List<Job>
    fun findByPriority(priority: JobPriority): List<Job>
    fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job>
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
    override fun findByTags(tags: Set<String>): List<Job> = delegate.findByTags(tags)
    override fun findByMetadata(key: String, value: Any): List<Job> = delegate.findByMetadata(key, value)
    override fun findByPriority(priority: JobPriority): List<Job> = delegate.findByPriority(priority)
    override fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job> = delegate.findByPriorityRange(min, max)
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

    override fun findByTags(tags: Set<String>): List<Job> {
        return jobs.values.filter { job -> job.tags.any { it in tags } }
    }

    override fun findByMetadata(key: String, value: Any): List<Job> {
        return jobs.values.filter { it.metadata[key] == value }
    }

    override fun findByPriority(priority: JobPriority): List<Job> {
        return jobs.values.filter { it.priority == priority }
    }

    override fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job> {
        return jobs.values.filter { it.priority in min..max }
    }

    override fun clear() {
        jobs.clear()
    }
}

/**
 * A JobRepository implementation that maintains jobs in a sorted set by priority.
 * Useful for scenarios where priority-based retrieval is frequent.
 */
class SortedJobRepository : JobRepository {
    private val idMap = ConcurrentHashMap<String, Job>()
    private val sortedJobs = ConcurrentSkipListSet<Job>()

    override fun save(job: Job) {
        remove(job.id)
        idMap[job.id] = job
        sortedJobs.add(job)
    }

    override fun findById(id: String): Job? = idMap[id]

    override fun remove(id: String): Job? {
        val job = idMap.remove(id)
        if (job != null) {
            sortedJobs.remove(job)
        }
        return job
    }

    override fun findAll(): Collection<Job> = sortedJobs

    override fun findByTag(tag: String): List<Job> = sortedJobs.filter { it.tags.contains(tag) }

    override fun findByTags(tags: Set<String>): List<Job> = sortedJobs.filter { job -> job.tags.any { it in tags } }

    override fun findByMetadata(key: String, value: Any): List<Job> = sortedJobs.filter { it.metadata[key] == value }

    override fun findByPriority(priority: JobPriority): List<Job> = sortedJobs.filter { it.priority == priority }

    override fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job> {
        return sortedJobs.filter { it.priority in min..max }
    }

    override fun clear() {
        idMap.clear()
        sortedJobs.clear()
    }
}

/**
 * A simple Map-based implementation of JobRepository.
 * Allows passing a custom mutable map for external management of stored jobs.
 */
class MapJobRepository(private val storage: MutableMap<String, Job> = ConcurrentHashMap()) : JobRepository {
    override fun save(job: Job) { storage[job.id] = job }
    override fun findById(id: String): Job? = storage[id]
    override fun remove(id: String): Job? = storage.remove(id)
    override fun findAll(): Collection<Job> = storage.values
    override fun findByTag(tag: String): List<Job> = storage.values.filter { it.tags.contains(tag) }
    override fun findByTags(tags: Set<String>): List<Job> = storage.values.filter { job -> job.tags.any { it in tags } }
    override fun findByMetadata(key: String, value: Any): List<Job> = storage.values.filter { it.metadata[key] == value }
    override fun findByPriority(priority: JobPriority): List<Job> = storage.values.filter { it.priority == priority }
    override fun findByPriorityRange(min: JobPriority, max: JobPriority): List<Job> = storage.values.filter { it.priority in min..max }
    override fun clear() = storage.clear()
}