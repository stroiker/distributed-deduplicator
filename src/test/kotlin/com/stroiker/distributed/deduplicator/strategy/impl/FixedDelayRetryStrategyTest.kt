package com.stroiker.distributed.deduplicator.strategy.impl

import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch
import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertThrows
import org.junitpioneer.jupiter.RetryingTest
import java.time.Duration
import java.util.concurrent.TimeUnit.MILLISECONDS

class FixedDelayRetryStrategyTest {

    private val strategy = FixedDelayRetryStrategy(3, Duration.ofMillis(10))

    @RetryingTest(maxAttempts = 5, name = "should retries 0 times")
    fun `should retries 0 times`() {
        var counter = 0
        val stopwatch = Stopwatch.createStarted()
        strategy.retry { counter++ }
        assertTrue(stopwatch.elapsed(MILLISECONDS) in 0..9)
        assertEquals(1, counter)
    }

    @RetryingTest(maxAttempts = 5, name = "should retries 0 times with other error")
    fun `should retries 0 times with other error`() {
        var counter = 0
        val stopwatch = Stopwatch.createStarted()
        assertThrows<RuntimeException> { strategy.retry { counter++; throw RuntimeException() } }
        assertTrue(stopwatch.elapsed(MILLISECONDS) in 0..9)
        assertEquals(1, counter)
    }

    @RetryingTest(maxAttempts = 5, name = "should retries 3 times with retry error")
    fun `should retries 3 times with retry error`() {
        var counter = 0
        val stopwatch = Stopwatch.createStarted()
        assertThrows<RetriesExceededException> { strategy.retry { counter++; throw RetryException("", "") } }
        assertTrue(stopwatch.elapsed(MILLISECONDS) in 30..39)
        assertEquals(4, counter)
    }
}
