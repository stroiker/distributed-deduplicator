package com.stroiker.distributed.deduplicator.strategy.async.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.strategy.async.RetryStrategyAsync
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

class FixedDelayRetryStrategyAsync(
    private val times: Int,
    private val delay: Duration,
    private val executor: ExecutorService
) : RetryStrategyAsync {

    override fun <T> retryAsync(action: () -> T): CompletableFuture<T> =
        CompletableFuture<T>().also { resultHandler -> retryAsync(times, action, resultHandler) }

    private fun <T> retryAsync(counter: Int, action: () -> T, resultHandler: CompletableFuture<T>) {
        runCatching {
            resultHandler.complete(action())
        }.onFailure { error ->
            if (error is RetryException) {
                if (counter == 0) {
                    resultHandler.completeExceptionally(RetriesExceededException(error.key, error.table))
                } else {
                    val delayed = CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS, executor)
                    CompletableFuture.supplyAsync({ retryAsync(counter - 1, action, resultHandler) }, delayed)
                }
            } else {
                resultHandler.completeExceptionally(error)
            }
        }
    }
}
