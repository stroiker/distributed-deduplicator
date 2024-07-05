package com.stroiker.distributed.deduplicator.strategy.sync.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class ExponentialDelayRetryStrategyTest {

    private val strategy = ExponentialDelayRetryStrategy(3, Duration.ofMillis(10))

    @Test
    fun `should retries 0 times`() {
        var counter = 0
        strategy.retry { counter++ }
        assertEquals(1, counter)
    }

    @Test
    fun `should retries 0 times with other error`() {
        var counter = 0
        assertThrows<RuntimeException> { strategy.retry { counter++; throw RuntimeException() } }
        assertEquals(1, counter)
    }

    @Test
    fun `should retries 3 times with retry error`() {
        var counter = 0
        assertThrows<RetriesExceededException> { strategy.retry { counter++; throw RetryException("", "") } }
        assertEquals(4, counter)
    }
}
