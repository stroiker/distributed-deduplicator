package com.stroiker.distributed.deduplicator.strategy.async

import java.util.concurrent.CompletableFuture

interface RetryStrategyAsync {

    fun <T> retryAsync(action: () -> T): CompletableFuture<T>
}
