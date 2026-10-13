package com.kalan.scheduler

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

/**
 * JobBatch allows for the definition of a sequence of jobs that should be executed in order.
 * If any job in the batch fails, the batch execution can be configured to stop or continue.
 */
class JobBatch(
    val batchId: String,
    val jobIds: List<String>,
    val stopOnFailure: Boolean = true
) {
    private val logger = Logger.getLogger(JobBatch::class.java.name)

    /**
     * Executes the jobs in the batch sequentially using the provided scheduler.
     * Returns a CompletableFuture that completes when the batch is finished.
     */
    fun execute(scheduler: KalanScheduler): CompletableFuture<BatchResult> {
        val resultFuture = CompletableFuture<BatchResult>()
        val executedJobs = mutableListOf<String>()
        val failures = mutableListOf<BatchFailure>()
        val currentJobIndex = AtomicInteger(0)

        fun runNextJob() {
            val index = currentJobIndex.getAndIncrement()
            if (index >= jobIds.size) {
                resultFuture.complete(BatchResult(
                    batchId = batchId,
                    successfulJobs = executedJobs,
                    failures = failures,
                    isSuccessful = failures.isEmpty()
                ))
                return
            }

            val jobId = jobIds[index]
            scheduler.executeNow(jobId)
            
            val job = scheduler.getJobInfo(jobId)
            if (job == null) {
                failures.add(BatchFailure(jobId, RuntimeException("Job not found in scheduler")))
                if (stopOnFailure) {
                    resultFuture.complete(BatchResult(batchId, executedJobs, failures, false))
                    return
                }
                runNextJob()
                return
            }

            CompletableFuture.runAsync {
                try {
                    var timeout = 0L
                    while (scheduler.getJobStatus(jobId) == JobStatus.RUNNING && timeout < 10000) {
                        Thread.sleep(50)
                        timeout += 50
                    }

                    if (scheduler.getJobStatus(jobId) == JobStatus.COMPLETED) {
                        executedJobs.add(jobId)
                        runNextJob()
                    } else {
                        val ex = RuntimeException("Job $jobId failed or timed out")
                        failures.add(BatchFailure(jobId, ex))
                        if (stopOnFailure) {
                            resultFuture.complete(BatchResult(batchId, executedJobs, failures, false))
                        } else {
                            runNextJob()
                        }
                    }
                } catch (e: Exception) {
                    failures.add(BatchFailure(jobId, e))
                    if (stopOnFailure) {
                        resultFuture.complete(BatchResult(batchId, executedJobs, failures, false))
                    } else {
                        runNextJob()
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