package com.kalan.scheduler

import java.util.concurrent.ConcurrentHashMap

interface JobRepository {
    fun save(job: Job)
    fun findById(id: String): Job?
    fun remove(id: String): Job?
    fun findAll(): Collection<Job>
    fun findByTag(tag: String): List<Job>
    fun clear()
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

    override fun clear() {
        jobs.clear()
    }
}