package com.stroiker.distributed.deduplicator.strategy.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.strategy.RetryStrategy
import java.time.Duration

class FixedDelayRetryStrategy(private val times: Int, private val delay: Duration) : RetryStrategy {

    override fun <T> retry(action: () -> T): T = retry(times, action)

    private fun <T> retry(counter: Int, action: () -> T): T =
        runCatching {
            action()
        }.getOrElse { error ->
            if (error is RetryException) {
                if (counter == 0) {
                    throw RetriesExceededException(error.key, error.table)
                } else {
                    Thread.sleep(delay.toMillis())
                    retry(counter - 1, action)
                }
            }
            throw error
        }
}
