package com.kalan.scheduler

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class Job(
    val id: String,
    @Volatile var action: (String) -> Unit, // Changed from () -> Unit to accept executionId
    val startTime: Instant,
    val intervalMs: Long? = null,
    @Volatile var priority: Int = 0,
    @Volatile var timeoutMs: Long? = null,
    val tags: Set<String> = emptySet(),
    val metadata: Map<String, Any> = emptyMap(),
    val maxRepetitions: Int? = null
) : Comparable<Job> {
    private val executionCount = AtomicInteger(0)
    private val lastExecutionTime = AtomicReference<Instant?>(null)
    private val paused = AtomicBoolean(false)

    fun setPaused(paused: Boolean) {
        this.paused.set(paused)
    }

    fun isPaused(): Boolean = paused.get()

    fun execute(): Boolean {
        if (paused.get()) return false
        
        if (isMaxRepetitionsReached()) {
            return false
        }

        val executionId = UUID.randomUUID().toString()
        action(executionId)
        executionCount.incrementAndGet()
        lastExecutionTime.set(Instant.now())
        return true
    }

    fun getExecutionCount(): Int = executionCount.get()
    
    fun getLastExecutionTime(): Instant? = lastExecutionTime.get()

    fun isMaxRepetitionsReached(): Boolean {
        return maxRepetitions != null && executionCount.get() >= maxRepetitions!!
    }

    override fun compareTo(other: Job):
        return other.priority.compareTo(this.priority) // Higher priority first
}