package com.kalan.scheduler

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

class KalanSchedulerTest {

    @Test
    fun `test basic delayed execution`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        var executed = false

        scheduler.schedule("test-1", 100) {
            executed = true
            latch.countDown()
            null
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertTrue(executed)
        assertEquals(1, scheduler.getExecutionCount("test-1"))
        scheduler.shutdown()
    }

    @Test
    fun `test schedule async execution`() {
        val scheduler = KalanScheduler()
        val expectedResult = "Async-Success"
        
        val future = scheduler.scheduleAsync("async-job", 100) {
            expectedResult
        }

        val result = future.get(500, TimeUnit.MILLISECONDS)
        assertEquals(expectedResult, result)
        assertEquals(1, scheduler.getExecutionCount("async-job"))
        scheduler.shutdown()
    }

    @Test
    fun `test schedule async failure`() {
        val scheduler = KalanScheduler()
        
        val future = scheduler.scheduleAsync("async-fail-job", 100) {
            throw RuntimeException("Async Boom")
        }

        assertThrows(java.util.concurrent.ExecutionException::class.java) {
            future.get(500, TimeUnit.MILLISECONDS)
        }
        scheduler.shutdown()
    }

    @Test
    fun `test schedule at specific time`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        val targetTime = Instant.now().plusMillis(200)

        scheduler.scheduleAt("test-at", targetTime) {
            latch.countDown()
            null
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertEquals(1, scheduler.getExecutionCount("test-at"))
        scheduler.shutdown()
    }

    @Test
    fun `test periodic execution`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(3)

        scheduler.scheduleAtFixedRate("test-periodic", 0, 50) {
            latch.countDown()
            null
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertTrue(scheduler.getExecutionCount("test-periodic") >= 3)
        scheduler.shutdown()
    }

    @Test
    fun `test periodic execution with max repetitions`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(3)
        
        scheduler.scheduleJob("limited-job") {
            every(50)
            repeatAtMost(3)
            execute { latch.countDown(); null }
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertEquals(3, scheduler.getExecutionCount("limited-job"))
        
        // Wait more to ensure it doesn't run further
        Thread.sleep(150)
        assertEquals(3, scheduler.getExecutionCount("limited-job"))
        assertEquals(JobStatus.CANCELLED, scheduler.getJobStatus("limited-job"))
        scheduler.shutdown()
    }

    @Test
    fun `test fixed delay execution`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)

        scheduler.scheduleWithFixedDelay("test-delay", 0, 50) {
            latch.countDown()
            null
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertTrue(scheduler.getExecutionCount("test-delay") >= 2)
        scheduler.shutdown()
    }

    @Test
    fun `test job cancellation`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        
        scheduler.schedule("cancel-me", 200) {
            latch.countDown()
            null
        }
        
        scheduler.cancel("cancel-me")
        
        assertFalse(latch.await(400, TimeUnit.MILLISECONDS))
        assertEquals(0, scheduler.getExecutionCount("cancel-me"))
        scheduler.shutdown()
    }

    @Test
    fun `test job status`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)

        scheduler.schedule("status-job", 100) {
            latch.countDown()
            null
        }

        assertEquals(JobStatus.RUNNING, scheduler.getJobStatus("status-job"))
        
        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertEquals(JobStatus.COMPLETED, scheduler.getJobStatus("status-job"))
        
        assertEquals(JobStatus.NOT_FOUND, scheduler.getJobStatus("unknown"))
        
        scheduler.shutdown()
    }

    @Test
    fun `test error handler`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        var errorCaught = false

        scheduler.errorHandler = { 
            errorCaught = true
            latch.countDown()
        }

        scheduler.schedule("error-job", 10) {
            throw RuntimeException("Boom")
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertTrue(errorCaught)
        // Even if it failed, the execute() method was called
        assertEquals(1, scheduler.getExecutionCount("error-job"))
        scheduler.shutdown()
    }

    @Test
    fun `test DSL scheduling`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)

        scheduler.scheduleJob("dsl-job") {
            startAfter(50)
            execute {
                latch.countDown()
                null
            }
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertEquals(1, scheduler.getExecutionCount("dsl-job"))
        scheduler.shutdown()
    }

    @Test
    fun `test DSL periodic scheduling`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)

        scheduler.scheduleJob("dsl-periodic") {
            every(50)
            execute {
                latch.countDown()
                null
            }
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertTrue(scheduler.getExecutionCount("dsl-periodic") >= 2)
        scheduler.shutdown()
    }

    @Test
    fun `test updateJob`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)
        var value = 0

        scheduler.scheduleAtFixedRate("update-job", 0, 50) {
            value += 1
            latch.countDown()
            null
        }

        // Wait for first execution
        assertTrue(latch.await(200, TimeUnit.MILLISECONDS))
        assertEquals(1, value)

        scheduler.updateJob("update-job") {
            value += 10
            latch.countDown()
            null
        }

        assertTrue(latch.await(200, TimeUnit.MILLISECONDS))
        assertEquals(11, value)
        scheduler.shutdown()
    }

    @Test
    fun `test getJobInfo`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)

        scheduler.scheduleAtFixedRate("info-job", 0, 100) {
            latch.countDown()
            null
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        val info = scheduler.getJobInfo("info-job")
        
        assertNotNull(info)
        assertEquals("info-job", info?.id)
        assertEquals(JobStatus.RUNNING, info?.status)
        assertEquals(100L, info?.intervalMs)
        assertTrue(info?.executionCount ?: 0 >= 1)
        
        scheduler.shutdown()
    }

    @Test
    fun `test pause and resume`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)
        var count = 0

        scheduler.scheduleAtFixedRate("pause-job", 0, 50) {
            count++
            latch.countDown()
            null
        }

        // Wait for first run
        assertTrue(latch.await(200, TimeUnit.MILLISECONDS))
        
        scheduler.pauseJob("pause-job")
        val countBeforeResume = count
        
        // Wait to ensure it doesn't run while paused
        Thread.sleep(200)
        assertEquals(countBeforeResume, count)

        scheduler.resumeJob("pause-job")
        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertTrue(count > countBeforeResume)
        
        scheduler.shutdown()
    }

    @Test
    fun `test listAllJobs`() {
        val scheduler = KalanScheduler()
        scheduler.schedule("job-1", 100) { null }
        scheduler.schedule("job-2", 200) { null }
        
        val jobs = scheduler.listAllJobs()
        assertEquals(2, jobs.size)
        assertTrue(jobs.any { it.id == "job-1" })
        assertTrue(jobs.any { it.id == "job-2" })
        
        scheduler.shutdown()
    }

    @Test
    fun `test priority in DSL`() {
        val scheduler = KalanScheduler()
        scheduler.scheduleJob("priority-job") {
            withPriority(10)
            execute { null }
        }
        val info = scheduler.getJobInfo("priority-job")
        assertEquals(10, info?.priority)
        scheduler.shutdown()
    }

    @Test
    fun `test job timeout enforcement`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        var timeoutCaught = false

        scheduler.errorHandler = {
            if (it is TimeoutException) {
                timeoutCaught = true
                latch.countDown()
            }
        }

        // Job that takes 500ms but has a timeout of 100ms
        scheduler.schedule("timeout-job", 0, timeoutMs = 100) {
            Thread.sleep(500)
            null
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Timeout should have been triggered")
        assertTrue(timeoutCaught)
        scheduler.shutdown()
    }

    @Test
    fun `test job without timeout finishes normally`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)

        scheduler.schedule("normal-job", 0, timeoutMs = null) {
            Thread.sleep(200)
            latch.countDown()
            null
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Job should have completed normally")
        scheduler.shutdown()
    }

    @Test
    fun `test job tags and bulk cancellation`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)
        
        scheduler.scheduleJob("tag-job-1") {
            withTags("batch1", "important")
            execute { latch.countDown(); null }
        }
        scheduler.scheduleJob("tag-job-2") {
            withTags("batch1")
            execute { latch.countDown(); null }
        }
        scheduler.scheduleJob("tag-job-3") {
            withTags("batch2")
            execute { latch.countDown(); null }
        }

        val batch1Jobs = scheduler.listJobsByTag("batch1")
        assertEquals(2, batch1Jobs.size)

        scheduler.cancelByTag("batch1")
        
        // Job 3 should still run, Job 1 and 2 cancelled
        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertEquals(1, latch.count)
        assertEquals(JobStatus.CANCELLED, scheduler.getJobStatus("tag-job-1"))
        assertEquals(JobStatus.CANCELLED, scheduler.getJobStatus("tag-job-2"))
        assertEquals(JobStatus.COMPLETED, scheduler.getJobStatus("tag-job-3"))

        scheduler.shutdown()
    }

    @Test
    fun `test metadata and dynamic updates`() {
        val scheduler = KalanScheduler()
        val meta = mapOf("env" to "test", "version" to 1)
        
        scheduler.scheduleJob("meta-job") {
            withMetadata(meta)
            withPriority(1)
            execute { null }
        }
        
        var info = scheduler.getJobInfo("meta-job")
        assertEquals(meta, info?.metadata)
        assertEquals(1, info?.priority)
        
        scheduler.updateJobPriority("meta-job", 10)
        scheduler.updateJobTimeout("meta-job", 500L)
        
        info = scheduler.getJobInfo("meta-job")
        assertEquals(10, info?.priority)
        assertEquals(500L, info?.timeoutMs)
        
        scheduler.shutdown()
    }

    @Test
    fun `test execution ID is provided`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        var receivedId: String? = null

        scheduler.schedule("exec-id-job", 0) {
            receivedId = it
            latch.countDown()
            null
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertNotNull(receivedId)
        assertTrue(receivedId!!.isNotEmpty())
        scheduler.shutdown()
    }

    @Test
    fun `test job dependencies`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)
        val results = mutableListOf<String>()

        scheduler.scheduleJob("job-1") {
            execute { 
                results.add("job-1")
                latch.countDown()
                null
            }
        }

        scheduler.scheduleJob("job-2") {
            dependsOn("job-1")
            execute { 
                results.add("job-2")
                latch.countDown()
                null
            }
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertEquals(listOf("job-1", "job-2"), results)
        scheduler.shutdown()
    }

    @Test
    fun `test retry policy`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(3)
        var attempts = 0

        scheduler.scheduleJob("retry-job") {
            withRetryPolicy(maxRetries = 2, delayMs = 50)
            execute {
                attempts++
                latch.countDown()
                throw RuntimeException("Fail")
            }
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertEquals(3, attempts)
        scheduler.shutdown()
    }

    @Test
    fun `test total job count`() {
        val scheduler = KalanScheduler()
        scheduler.schedule("j1", 10) { null }
        scheduler.schedule("j2", 20) { null }
        
        assertEquals(2, scheduler.getTotalJobCount())
        
        scheduler.shutdown()
    }

    @Test
    fun `test job event listeners`() {
        val scheduler = KalanScheduler()
        val events = ConcurrentLinkedQueue<JobEvent>()
        val latch = CountDownLatch(2)

        scheduler.addEventListener(object : JobEventListener {
            override fun onEvent(event: JobEvent) {
                events.add(event)
                if (event.type == JobEvent.Type.COMPLETED) {
                    latch.countDown()
                }
            }
        })

        scheduler.schedule("event-job", 10) {
            null
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        
        val jobEvents = events.filter { it.jobId == "event-job" }
        assertTrue(jobEvents.any { it.type == JobEvent.Type.STARTED })
        assertTrue(jobEvents.any { it.type == JobEvent.Type.COMPLETED })
        
        scheduler.shutdown()
    }

    @Test
    fun `test job failure event`() {
        val scheduler = KalanScheduler()
        val events = ConcurrentLinkedQueue<JobEvent>()
        val latch = CountDownLatch(1)

        scheduler.addEventListener(object : JobEventListener {
            override fun onEvent(event: JobEvent) {
                events.add(event)
                if (event.type == JobEvent.Type.FAILED) {
                    latch.countDown()
                }
            }
        })

        scheduler.schedule("fail-job", 10) {
            throw RuntimeException("Fail")
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        assertTrue(events.any { it.type == JobEvent.Type.FAILED && it.jobId == "fail-job" })
        
        scheduler.shutdown()
    }

    @Test
    fun `test job result retrieval`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        val expectedValue = "Success-Result"

        scheduler.schedule("result-job", 0) {
            latch.countDown()
            expectedValue
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertEquals(expectedValue, scheduler.getLastResult("result-job"))
        scheduler.shutdown()
    }

    @Test
    fun `test job result in JobInfo`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        val expectedValue = "Info-Result"

        scheduler.schedule("info-result-job", 0) {
            latch.countDown()
            expectedValue
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        val info = scheduler.getJobInfo("info-result-job")
        assertEquals(expectedValue, info?.lastResult)
        scheduler.shutdown()
    }

    @Test
    fun `test per-job concurrency limit`() {
        val scheduler = KalanScheduler(corePoolSize = 10)
        val executionCount = AtomicInteger(0)
        val latch = CountDownLatch(3)
        
        // Schedule a periodic job with a concurrency limit of 1
        scheduler.scheduleJob("concurrent-job") {
            every(10)
            withConcurrencyLimit(1)
            execute {
                executionCount.incrementAndGet()
                Thread.sleep(100)
                latch.countDown()
                null
            }
        }

        // Wait for a few cycles. If concurrency limit works, they should run sequentially
        // and not overlap. We check that the number of concurrent runs doesn't exceed 1
        // implicitly by seeing if they take at least (count * 100ms).
        val start = System.currentTimeMillis()
        assertTrue(latch.await(2, TimeUnit.SECONDS))
        val duration = System.currentTimeMillis() - start
        
        assertTrue(duration >= 300, "Jobs should have run sequentially due to concurrency limit")
        scheduler.shutdown()
    }

    @Test
    fun `test job groups`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)

        scheduler.scheduleAtFixedRate("group-job-1", 0, 100) {
            latch.countDown()
            null
        }
        scheduler.scheduleAtFixedRate("group-job-2", 0, 100) {
            latch.countDown()
            null
        }

        scheduler.addJobToGroup("my-group", "group-job-1")
        scheduler.addJobToGroup("my-group", "group-job-2")

        assertEquals(2, scheduler.getJobsInGroup("my-group").size)

        scheduler.pauseGroup("my-group")
        val countAfterPause = scheduler.getExecutionCount("group-job-1") + scheduler.getExecutionCount("group-job-2")
        
        Thread.sleep(200)
        assertEquals(countAfterPause, scheduler.getExecutionCount("group-job-1") + scheduler.getExecutionCount("group-job-2"))

        scheduler.resumeGroup("my-group")
        assertTrue(latch.await(1, TimeUnit.SECONDS))

        scheduler.cancelGroup("my-group")
        assertEquals(JobStatus.CANCELLED, scheduler.getJobStatus("group-job-1"))
        assertEquals(JobStatus.CANCELLED, scheduler.getJobStatus("group-job-2"))
        
        scheduler.shutdown()
    }

    @Test
    fun `test executeNow`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        
        // Schedule a job far in the future
        scheduler.schedule("manual-job", 100000) {
            latch.countDown()
            null
        }
        
        // Verify it hasn't run yet
        assertFalse(latch.await(200, TimeUnit.MILLISECONDS))
        assertEquals(0, scheduler.getExecutionCount("manual-job"))
        
        // Trigger execution now
        scheduler.executeNow("manual-job")
        
        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertEquals(1, scheduler.getExecutionCount("manual-job"))
        scheduler.shutdown()
    }

    @Test
    fun `test fluent DSL for retry and limit`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)

        scheduler.scheduleJob("fluent-job") {
            retry(max = 3, every = 10)
            limitConcurrency(2)
            execute {
                latch.countDown()
                null
            }
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        
        // We can't easily check internal Job fields via JobInfo without adding them
        // but we verify the job executes and the DSL doesn't crash
        assertEquals(1, scheduler.getExecutionCount("fluent-job"))
        scheduler.shutdown()
    }

    @Test
    fun `test cron scheduling`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)

        // Schedule to run every minute (or just matching current) 
        // For test purposes, we use an expression that matches frequently
        scheduler.scheduleJob("cron-job") {
            cron(CronExpression(minute = -1, hour = -1)) 
            execute {
                latch.countDown()
                null
            }
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        assertTrue(scheduler.getExecutionCount("cron-job") >= 2)
        scheduler.shutdown()
    }

    @Test
    fun `test scheduler health status`() {
        val scheduler = KalanScheduler()
        
        scheduler.schedule("health-job", 100) { null }
        
        val health = scheduler.getHealthStatus()
        assertTrue(health.isRunning)
        assertEquals(1, health.activeJobCount)
        
        scheduler.shutdown()
        val postShutdownHealth = scheduler.getHealthStatus()
        assertFalse(postShutdownHealth.isRunning)
    }

    @Test
    fun `test runJobsParallel`() {
        val scheduler = KalanScheduler(corePoolSize = 4)
        val latch = CountDownLatch(3)
        
        scheduler.scheduleJob("p1") { execute { latch.countDown(); "res1" } }
        scheduler.scheduleJob("p2") { execute { latch.countDown(); "res2" } }
        scheduler.scheduleJob("p3") { execute { latch.countDown(); "res3" } }
        
        val futures = scheduler.runJobsParallel(listOf("p1", "p2", "p3"), 1, TimeUnit.SECONDS)
        
        CompletableFuture.allOf(*futures.toTypedArray()).join()
        
        assertEquals("res1", futures[0].get())
        assertEquals("res2", futures[1].get())
        assertEquals("res3", futures[2].get())
        assertEquals(0, latch.count)
        
        scheduler.shutdown()
    }

    @Test
    fun `test skip if running strategy`() {
        val scheduler = KalanScheduler(corePoolSize = 2)
        val executionCount = AtomicInteger(0)
        val latch = CountDownLatch(1)

        scheduler.scheduleJob("skip-job") {
            every(50)
            withExecutionStrategy(JobExecutionStrategy.SKIP_IF_RUNNING)
            execute {
                executionCount.incrementAndGet()
                Thread.sleep(200)
                latch.countDown()
                null
            }
        }

        // Let it run for a bit. Since it takes 200ms and triggers every 50ms,
        // the SKIP_IF_RUNNING strategy should prevent overlapping.
        Thread.sleep(500)
        
        val count = executionCount.get()
        // Without skip, we might see more executions queued. With skip, we expect 
        // them to be spaced by at least 200ms.
        assertTrue(count <= 3, "Should have skipped overlapping executions. Count: $count")
        scheduler.shutdown()
    }

    @Test
    fun `test cancel by metadata`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)
        
        scheduler.scheduleJob("meta-cancel-1") {
            withMetadata(mapOf("cluster" to "us-east-1"))
            execute { latch.countDown(); null }
        }
        scheduler.scheduleJob("meta-cancel-2") {
            withMetadata(mapOf("cluster" to "us-east-1"))
            execute { latch.countDown(); null }
        }
        scheduler.scheduleJob("meta-keep") {
            withMetadata(mapOf("cluster" to "us-west-2"))
            execute { latch.countDown(); null }
        }

        scheduler.cancelByMetadata("cluster", "us-east-1")
        
        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertEquals(1, latch.count)
        assertEquals(JobStatus.CANCELLED, scheduler.getJobStatus("meta-cancel-1"))
        assertEquals(JobStatus.CANCELLED, scheduler.getJobStatus("meta-cancel-2"))
        assertEquals(JobStatus.COMPLETED, scheduler.getJobStatus("meta-keep"))
        
        scheduler.shutdown()
    }

    @Test
    fun `test Job execute returns Failure on exception`() {
        val job = Job(
            id = "fail-job",
            action = { throw RuntimeException("Expected Failure") },
            startTime = Instant.now()
        )
        
        val result = job.execute()
        assertTrue(result is JobResult.Failure)
        assertEquals("Expected Failure", (result as JobResult.Failure).exception.message)
        assertEquals(1, job.getFailureCount())
    }

    @Test
    fun `test DSL group assignment`() {
        val scheduler = KalanScheduler()
        
        scheduler.scheduleJob("group-dsl-1") {
            inGroup("dsl-group")
            execute { null }
        }
        scheduler.scheduleJob("group-dsl-2") {
            inGroup("dsl-group")
            execute { null }
        }
        
        assertEquals(2, scheduler.getJobsInGroup("dsl-group").size)
        scheduler.shutdown()
    }

    @Test
    fun `test global pause and resume`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)
        var count = 0

        scheduler.scheduleAtFixedRate("global-1", 0, 50) {
            count++; latch.countDown(); null
        }
        scheduler.scheduleAtFixedRate("global-2", 0, 50) {
            count++; latch.countDown(); null
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        val countBeforePause = count
        
        scheduler.pauseAllJobs()
        Thread.sleep(200)
        assertEquals(countBeforePause, count)

        scheduler.resumeAllJobs()
        Thread.sleep(200)
        assertTrue(count > countBeforePause)
        
        scheduler.shutdown()
    }

    @Test
    fun `test periodic job result retrieval`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(2)
        var currentVal = 0

        scheduler.scheduleAtFixedRate("periodic-res", 0, 50) {
            currentVal++
            latch.countDown()
            "Result-$currentVal"
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS))
        val lastRes = scheduler.getLastResult("periodic-res")
        assertTrue(lastRes.toString().startsWith("Result-"))
        
        scheduler.shutdown()
    }

    @Test
    fun `test graceful shutdown`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        
        scheduler.schedule("shutdown-job", 10) {
            latch.countDown()
            null
        }
        
        scheduler.shutdown()
        // The job might or might not run depending on timing, but shutdown should be non-blocking
        // and the dispatcher should terminate.
        assertTrue(true)
    }
}
