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
}