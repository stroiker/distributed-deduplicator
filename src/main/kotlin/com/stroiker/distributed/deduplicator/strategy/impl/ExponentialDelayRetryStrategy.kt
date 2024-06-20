package com.stroiker.distributed.deduplicator.strategy.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.strategy.RetryStrategy
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.exp

class ExponentialDelayRetryStrategy(private val times: Int, private val initialDelay: Duration) : RetryStrategy {

    private val threadLocalCounter: ThreadLocal<AtomicInteger> = ThreadLocal.withInitial { AtomicInteger(0) }

    override fun <T> retry(action: () -> T): T =
        runCatching {
            action().also { threadLocalCounter.remove() }
        }.getOrElse { error ->
            if (error is RetryException) {
                val retryCounter = threadLocalCounter.get().incrementAndGet()
                if (retryCounter <= times) {
                    Thread.sleep(computeDelayMillis(retryCounter, initialDelay))
                    retry(action)
                } else {
                    threadLocalCounter.remove()
                    throw throw RetriesExceededException(error.key, error.table)
                }
            } else {
                threadLocalCounter.remove()
                throw error
            }
        }

    private fun computeDelayMillis(retryCount: Int, delay: Duration): Long =
        if (retryCount == 0) delay.toMillis() else (delay.toMillis() * exp(retryCount.toDouble())).toLong()
}
