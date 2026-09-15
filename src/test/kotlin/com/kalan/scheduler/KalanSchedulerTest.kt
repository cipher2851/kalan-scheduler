package com.kalan.scheduler

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class KalanSchedulerTest {

    @Test
    fun `test basic delayed execution`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        var executed = false

        scheduler.schedule("test-1", 100) {
            executed = true
            latch.countDown()
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertTrue(executed)
        assertEquals(1, scheduler.getExecutionCount("test-1"))
        scheduler.shutdown()
    }

    @Test
    fun `test schedule at specific time`() {
        val scheduler = KalanScheduler()
        val latch = CountDownLatch(1)
        val targetTime = Instant.now().plusMillis(200)

        scheduler.scheduleAt("test-at", targetTime) {
            latch.countDown()
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
            execute { latch.countDown() }
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
        }

        // Wait for first execution
        assertTrue(latch.await(200, TimeUnit.MILLISECONDS))
        assertEquals(1, value)

        scheduler.updateJob("update-job") {
            value += 10
            latch.countDown()
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
        scheduler.schedule("job-1", 100) {}
        scheduler.schedule("job-2", 200) {}
        
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
            execute {}
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
            execute { latch.countDown() }
        }
        scheduler.scheduleJob("tag-job-2") {
            withTags("batch1")
            execute { latch.countDown() }
        }
        scheduler.scheduleJob("tag-job-3") {
            withTags("batch2")
            execute { latch.countDown() }
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
            execute {}
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
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS))
        assertNotNull(receivedId)
        assertTrue(receivedId!!.isNotEmpty())
        scheduler.shutdown()
    }
}