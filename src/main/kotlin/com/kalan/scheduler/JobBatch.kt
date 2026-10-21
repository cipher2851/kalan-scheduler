package com.kalan.scheduler

import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger

/**
 * JobBatch allows for the definition of a sequence of jobs that should be executed in order.
 * If any job in the batch fails, the batch execution can be configured to stop or continue.
 */
class JobBatch(
    val batchId: String,
    val jobIds: List<String>,
    val stopOnFailure: Boolean = true,
    val timeoutMs: Long = 10000
) {
    private val logger = Logger.getLogger(JobBatch::class.java.name)
    private val safeTimeoutMs = if (timeoutMs < 0) 0L else timeoutMs
    private val currentJobId = AtomicReference<String?>(null)

    /**
     * Executes the jobs in the batch sequentially using the provided scheduler.
     * Returns a CompletableFuture that completes when the batch is finished.
     */
    fun execute(scheduler: KalanScheduler): CompletableFuture<BatchResult> {
        val resultFuture = CompletableFuture<BatchResult>()
        val executedJobs = java.util.concurrent.CopyOnWriteArrayList<String>()
        val failures = java.util.concurrent.CopyOnWriteArrayList<BatchFailure>()
        val currentJobIndex = AtomicInteger(0)
        val completed = AtomicBoolean(false)

        resultFuture.whenComplete { _, throwable ->
            if (throwable is CancellationException) {
                completed.set(true)
                currentJobId.get()?.let { jobId ->
                    try {
                        scheduler.cancel(jobId)
                    } catch (e: Exception) {
                        logger.warning("Failed to cancel job $jobId after batch cancellation: ${e.message}")
                    }
                }
            }
        }

        fun finish(result: BatchResult) {
            if (completed.compareAndSet(false, true)) {
                resultFuture.complete(result)
            }
        }

        fun computeDeadline(): Long {
            val now = System.currentTimeMillis()
            if (safeTimeoutMs == Long.MAX_VALUE || now > Long.MAX_VALUE - safeTimeoutMs) {
                return Long.MAX_VALUE
            }
            return now + safeTimeoutMs
        }

        fun runNextJob() {
            if (completed.get() || resultFuture.isCancelled) {
                if (resultFuture.isCancelled) completed.set(true)
                return
            }
            val index = currentJobIndex.getAndIncrement()
            if (index >= jobIds.size) {
                finish(
                    BatchResult(
                        batchId = batchId,
                        successfulJobs = executedJobs.toList(),
                        failures = failures.toList(),
                        isSuccessful = failures.isEmpty()
                    )
                )
                return
            }

            val jobId = jobIds[index]
            currentJobId.set(jobId)
            try {
                scheduler.executeNow(jobId)
            } catch (e: Exception) {
                failures.add(BatchFailure(jobId, e))
                if (stopOnFailure) {
                    finish(BatchResult(batchId, executedJobs.toList(), failures.toList(), false))
                    return
                }
                CompletableFuture.runAsync { runNextJob() }
                return
            }

            val job = scheduler.getJobInfo(jobId)
            if (job == null) {
                failures.add(BatchFailure(jobId, RuntimeException("Job not found in scheduler")))
                if (stopOnFailure) {
                    finish(BatchResult(batchId, executedJobs.toList(), failures.toList(), false))
                    return
                }
                CompletableFuture.runAsync { runNextJob() }
                return
            }

            CompletableFuture.runAsync {
                if (completed.get() || resultFuture.isCancelled) {
                    if (resultFuture.isCancelled) {
                        completed.set(true)
                        try {
                            scheduler.cancel(jobId)
                        } catch (e: Exception) {
                            logger.warning("Failed to cancel timed out job $jobId: ${e.message}")
                        }
                    }
                    return@runAsync
                }
                try {
                    val deadline = computeDeadline()
                    var interrupted = false
                    while (!completed.get() &&
                        !resultFuture.isCancelled &&
                        scheduler.getJobStatus(jobId) == JobStatus.RUNNING &&
                        System.currentTimeMillis() < deadline
                    ) {
                        try {
                            Thread.sleep(50)
                        } catch (ie: InterruptedException) {
                            Thread.currentThread().interrupt()
                            interrupted = true
                            break
                        }
                    }

                    if (completed.get() || resultFuture.isCancelled) {
                        if (resultFuture.isCancelled) {
                            completed.set(true)
                            try {
                                scheduler.cancel(jobId)
                            } catch (e: Exception) {
                                logger.warning("Failed to cancel timed out job $jobId: ${e.message}")
                            }
                        }
                        return@runAsync
                    }

                    val status = try {
                        scheduler.getJobStatus(jobId)
                    } catch (e: Exception) {
                        null
                    }

                    if (status == JobStatus.COMPLETED) {
                        executedJobs.add(jobId)
                        runNextJob()
                    } else {
                        if (status == JobStatus.RUNNING) {
                            try {
                                scheduler.cancel(jobId)
                            } catch (e: Exception) {
                                logger.warning("Failed to cancel timed out job $jobId: ${e.message}")
                            }
                        }
                        val reason = if (interrupted) {
                            "Job $jobId interrupted while waiting for completion"
                        } else if (status == JobStatus.RUNNING) {
                            "Job $jobId timed out (status=$status)"
                        } else {
                            "Job $jobId failed or timed out (status=$status)"
                        }
                        failures.add(BatchFailure(jobId, RuntimeException(reason)))
                        if (stopOnFailure) {
                            finish(BatchResult(batchId, executedJobs.toList(), failures.toList(), false))
                        } else {
                            CompletableFuture.runAsync { runNextJob() }
                        }
                    }
                } catch (e: Exception) {
                    if (completed.get() || resultFuture.isCancelled) {
                        if (resultFuture.isCancelled) completed.set(true)
                        return@runAsync
                    }
                    failures.add(BatchFailure(jobId, e))
                    if (stopOnFailure) {
                        finish(BatchResult(batchId, executedJobs.toList(), failures.toList(), false))
                    } else {
                        CompletableFuture.runAsync { runNextJob() }
                    }
                }
            }
        }

        runNextJob()
        return resultFuture
    }

    data class BatchResult(
        val batchId: String,
        val successfulJobs: List<String>,
        val failures: List<BatchFailure>,
        val isSuccessful: Boolean
    )

    data class BatchFailure(
        val jobId: String,
        val exception: Throwable
    )
}
