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
        scheduler.shutdown()
    }
}