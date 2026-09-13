package com.kalan.scheduler

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

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
}