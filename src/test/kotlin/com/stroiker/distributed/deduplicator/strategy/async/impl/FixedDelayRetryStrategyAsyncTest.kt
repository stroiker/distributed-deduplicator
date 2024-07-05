package com.stroiker.distributed.deduplicator.strategy.async.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.util.concurrent.CompletionException
import java.util.concurrent.Executors

class FixedDelayRetryStrategyAsyncTest {

    private val strategy = FixedDelayRetryStrategyAsync(3, Duration.ofMillis(10), Executors.newSingleThreadExecutor())

    @Test
    fun `should retries 0 times`() {
        var counter = 0
        strategy.retryAsync { counter++ }.join()
        Assertions.assertEquals(1, counter)
    }

    @Test
    fun `should retries 0 times with other error`() {
        var counter = 0
        assertThrows<RuntimeException> { strategy.retryAsync { counter++; throw RuntimeException() }.join() }
        Assertions.assertEquals(1, counter)
    }

    @Test
    fun `should retries 3 times with retry error`() {
        var counter = 0
        assertThrows<CompletionException> { strategy.retryAsync { counter++; throw RetryException("", "") }.join() }.also { error ->
            assertTrue(error.cause is RetriesExceededException)
        }
        Assertions.assertEquals(4, counter)
    }
}
