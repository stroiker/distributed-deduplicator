package com.stroiker.distributed.deduplicator.strategy.async.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.strategy.async.RetryStrategyAsync
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import kotlin.math.exp

class ExponentialDelayRetryStrategyAsync(
    private val times: Int,
    private val initialDelay: Duration,
    private val executor: ExecutorService
) : RetryStrategyAsync {

    override fun <T> retryAsync(action: () -> T): CompletableFuture<T> =
        CompletableFuture<T>().also { resultHandler -> retryAsync(0, action, resultHandler) }

    private fun <T> retryAsync(counter: Int, action: () -> T, resultHandler: CompletableFuture<T>) {
        runCatching {
            resultHandler.complete(action())
        }.onFailure { error ->
            if (error is RetryException) {
                if (counter == times) {
                    resultHandler.completeExceptionally(RetriesExceededException(error.key, error.table))
                } else {
                    val delayed = CompletableFuture.delayedExecutor(computeDelayMillis(counter), TimeUnit.MILLISECONDS, executor)
                    CompletableFuture.supplyAsync({ retryAsync(counter + 1, action, resultHandler) }, delayed)
                }
            } else {
                resultHandler.completeExceptionally(error)
            }
        }
    }

    private fun computeDelayMillis(retryCount: Int): Long =
        if (retryCount == 0) initialDelay.toMillis() else (initialDelay.toMillis() * exp(retryCount.toDouble())).toLong()
}
