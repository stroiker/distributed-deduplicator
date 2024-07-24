package com.stroiker.distributed.deduplicator.provider

import com.datastax.oss.driver.api.core.CqlSession
import com.stroiker.distributed.deduplicator.absorber.DuplicateBurstAbsorber
import com.stroiker.distributed.deduplicator.strategy.async.RetryStrategyAsync
import com.stroiker.distributed.deduplicator.strategy.sync.RetryStrategy
import java.time.Duration
import java.util.concurrent.CompletableFuture

class DeduplicationProviderAsync internal constructor(
    session: CqlSession,
    profileName: String,
    absorber: DuplicateBurstAbsorber,
    private val strategy: RetryStrategyAsync
) : DeduplicationProvider(session, profileName, absorber, RetryStrategyAdapter()) {

    fun <T> processAsync(
        key: String,
        table: String,
        keyspace: String,
        ttl: Duration,
        block: () -> T
    ): CompletableFuture<T> = strategy.retryAsync { super.process(key, table, keyspace, ttl, block) }

    override fun <T> process(key: String, table: String, keyspace: String, ttl: Duration, block: () -> T): T {
        throw UnsupportedOperationException("Synchronous method is unsupported for async provider")
    }

    private class RetryStrategyAdapter : RetryStrategy {
        override fun <T> retry(action: () -> T): T = action.invoke()
    }
}
