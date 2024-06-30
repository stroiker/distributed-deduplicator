package com.stroiker.distributed.deduplicator.strategy.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.strategy.RetryStrategy
import java.time.Duration
import kotlin.math.exp

class ExponentialDelayRetryStrategy(private val times: Int, private val initialDelay: Duration) : RetryStrategy {

    override fun <T> retry(action: () -> T): T = retry(0, action)

    private fun <T> retry(counter: Int, action: () -> T): T =
        runCatching {
            action()
        }.getOrElse { error ->
            if (error is RetryException) {
                if (counter == times) {
                    throw RetriesExceededException(error.key, error.table)
                } else {
                    Thread.sleep(computeDelayMillis(counter))
                    retry(counter + 1, action)
                }
            }
            throw error
        }

    private fun computeDelayMillis(retryCount: Int): Long =
        if (retryCount == 0) initialDelay.toMillis() else (initialDelay.toMillis() * exp(retryCount.toDouble())).toLong()
}
