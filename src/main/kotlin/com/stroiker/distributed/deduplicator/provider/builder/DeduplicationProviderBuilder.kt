package com.stroiker.distributed.deduplicator.provider.builder

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile
import com.stroiker.distributed.deduplicator.Utils.getRequestTimeout
import com.stroiker.distributed.deduplicator.absorber.DuplicateBurstAbsorber
import com.stroiker.distributed.deduplicator.absorber.impl.CachedDuplicateBurstAbsorber
import com.stroiker.distributed.deduplicator.absorber.impl.NoDuplicateBurstAbsorber
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider
import com.stroiker.distributed.deduplicator.provider.DeduplicationProviderAsync
import com.stroiker.distributed.deduplicator.strategy.async.RetryStrategyAsync
import com.stroiker.distributed.deduplicator.strategy.async.impl.ExponentialDelayRetryStrategyAsync
import com.stroiker.distributed.deduplicator.strategy.sync.RetryStrategy
import com.stroiker.distributed.deduplicator.strategy.sync.impl.ExponentialDelayRetryStrategy
import java.time.Duration
import java.util.concurrent.Executors

class DeduplicationProviderBuilder {

    class DeduplicationProviderSyncBuilder {

        private var session: Lazy<CqlSession> = lazy {
            CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromClasspath("application.conf"))
                .build()
        }
        private var strategy: Lazy<RetryStrategy> = lazy {
            ExponentialDelayRetryStrategy(
                3,
                session.value.getRequestTimeout(profileName).multipliedBy(2)
            )
        }
        private var profileName: String = DriverExecutionProfile.DEFAULT_NAME
        private var absorber: DuplicateBurstAbsorber = NoDuplicateBurstAbsorber()

        fun withSession(session: CqlSession): DeduplicationProviderSyncBuilder {
            this.session = lazyOf(session)
            return this
        }

        fun withRetryStrategy(strategy: RetryStrategy): DeduplicationProviderSyncBuilder {
            this.strategy = lazyOf(strategy)
            return this
        }

        fun withSessionProfile(profileName: String): DeduplicationProviderSyncBuilder {
            this.profileName = profileName
            return this
        }

        fun withDuplicateAbsorber(absorberSize: Long, absorbTime: Duration): DeduplicationProviderSyncBuilder {
            this.absorber = CachedDuplicateBurstAbsorber(absorberSize, absorbTime)
            return this
        }

        fun build(): DeduplicationProvider = DeduplicationProvider(session.value, profileName, absorber, strategy.value)
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
        private var absorber: DuplicateBurstAbsorber = NoDuplicateBurstAbsorber()

        fun withSession(session: CqlSession): DeduplicationProviderAsyncBuilder {
            this.session = lazyOf(session)
            return this
        }

        fun withRetryStrategy(strategy: RetryStrategyAsync): DeduplicationProviderAsyncBuilder {
            this.strategy = lazyOf(strategy)
            return this
        }

        fun withSessionProfile(profileName: String): DeduplicationProviderAsyncBuilder {
            this.profileName = profileName
            return this
        }

        fun withDuplicateAbsorber(absorberSize: Long, absorbTime: Duration): DeduplicationProviderAsyncBuilder {
            this.absorber = CachedDuplicateBurstAbsorber(absorberSize, absorbTime)
            return this
        }

        fun build(): DeduplicationProviderAsync = DeduplicationProviderAsync(session.value, profileName, absorber, strategy.value)
    }

    companion object {
        fun newProviderBuilder(): DeduplicationProviderSyncBuilder = DeduplicationProviderSyncBuilder()
        fun newAsyncProviderBuilder(): DeduplicationProviderAsyncBuilder = DeduplicationProviderAsyncBuilder()
    }
}
