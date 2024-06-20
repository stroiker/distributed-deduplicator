package com.stroiker.distributed.deduplicator.strategy.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.strategy.RetryStrategy
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class FixedDelayRetryStrategy(private val times: Int, private val delay: Duration) : RetryStrategy {

    private val threadLocalCounter: ThreadLocal<AtomicInteger> = ThreadLocal.withInitial { AtomicInteger(times) }

    override fun <T> retry(action: () -> T): T =
        runCatching {
            action().also { threadLocalCounter.remove() }
        }.getOrElse { error ->
            if (error is RetryException) {
                if (threadLocalCounter.get().decrementAndGet() >= 0) {
                    Thread.sleep(delay.toMillis())
                    retry(action)
                } else {
                    threadLocalCounter.remove()
                    throw RetriesExceededException(error.key, error.table)
                }
            } else {
                threadLocalCounter.remove()
                throw error
            }
        }
}
