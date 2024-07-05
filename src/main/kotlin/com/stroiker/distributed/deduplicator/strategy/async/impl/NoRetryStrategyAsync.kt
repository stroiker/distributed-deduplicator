package com.stroiker.distributed.deduplicator.strategy.async.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.strategy.async.RetryStrategyAsync
import java.util.concurrent.CompletableFuture

class NoRetryStrategyAsync : RetryStrategyAsync {

    override fun <T> retryAsync(action: () -> T): CompletableFuture<T> =
        CompletableFuture<T>().also { resultHandler ->
            runCatching { resultHandler.complete(action.invoke()) }
                .onFailure { error ->
                    when (error) {
                        is RetryException -> resultHandler.completeExceptionally(
                            RetriesExceededException(
                                error.key,
                                error.table
                            )
                        )
                        else -> resultHandler.completeExceptionally(error)
                    }
                }
        }
}
