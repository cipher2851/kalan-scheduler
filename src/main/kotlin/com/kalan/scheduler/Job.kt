package com.kalan.scheduler

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

class Job(
    val id: String,
    val action: () -> Unit,
    val startTime: Instant,
    val intervalMs: Long? = null
) {
    private val executionCount = AtomicInteger(0)

    fun execute() {
        action()
        executionCount.incrementAndGet()
    }

    fun getExecutionCount(): Int = executionCount.get()
}