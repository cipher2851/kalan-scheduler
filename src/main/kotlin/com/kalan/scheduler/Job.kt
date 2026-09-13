package com.kalan.scheduler

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class Job(
    val id: String,
    @Volatile var action: () -> Unit,
    val startTime: Instant,
    val intervalMs: Long? = null
) {
    private val executionCount = AtomicInteger(0)
    private val lastExecutionTime = AtomicReference<Instant?>(null)

    fun execute() {
        action()
        executionCount.incrementAndGet()
        lastExecutionTime.set(Instant.now())
    }

    fun getExecutionCount(): Int = executionCount.get()
    
    fun getLastExecutionTime(): Instant? = lastExecutionTime.get()
}