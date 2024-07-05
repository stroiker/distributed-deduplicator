package com.stroiker.distributed.deduplicator.provider

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile
import com.stroiker.distributed.deduplicator.Utils.getRequestTimeout
import com.stroiker.distributed.deduplicator.strategy.async.RetryStrategyAsync
import com.stroiker.distributed.deduplicator.strategy.async.impl.ExponentialDelayRetryStrategyAsync
import com.stroiker.distributed.deduplicator.strategy.sync.RetryStrategy
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

class DeduplicationProviderAsync private constructor(
    session: CqlSession,
    profileName: String,
    private val retryStrategy: RetryStrategyAsync
) : DeduplicationProvider(session, profileName, RetryStrategyAdapter()) {

    fun <T> processAsync(
        key: String,
        table: String,
        keyspace: String,
        ttl: Duration,
        block: () -> T
    ): CompletableFuture<T> = retryStrategy.retryAsync { super.process(key, table, keyspace, ttl, block) }

    override fun <T> process(key: String, table: String, keyspace: String, ttl: Duration, block: () -> T): T {
        throw UnsupportedOperationException("Synchronous method is unsupported for async provider")
    }

    private class RetryStrategyAdapter : RetryStrategy {
        override fun <T> retry(action: () -> T): T = action.invoke()
    }

    class DeduplicationProviderAsyncBuilder {

        private var session: Lazy<CqlSession> = lazy {
            CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromClasspath("application.conf"))
                .build()
        }
        private var strategy: Lazy<RetryStrategyAsync> = lazy {
            ExponentialDelayRetryStrategyAsync(
                3,
                session.value.getRequestTimeout(profileName).multipliedBy(2),
                Executors.newWorkStealingPool()
            )
        }
        private var profileName: String = DriverExecutionProfile.DEFAULT_NAME

        fun session(session: CqlSession): DeduplicationProviderAsyncBuilder {
            this.session = lazyOf(session)
            return this
        }

        fun strategy(strategy: RetryStrategyAsync): DeduplicationProviderAsyncBuilder {
            this.strategy = lazyOf(strategy)
            return this
        }

        fun profile(profileName: String): DeduplicationProviderAsyncBuilder {
            this.profileName = profileName
            return this
        }

        fun build(): DeduplicationProviderAsync = DeduplicationProviderAsync(session.value, profileName, strategy.value)
    }

    companion object {
        fun builder() = DeduplicationProviderAsyncBuilder()
    }
}
