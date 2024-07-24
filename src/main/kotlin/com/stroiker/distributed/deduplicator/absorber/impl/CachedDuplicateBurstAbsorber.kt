package com.stroiker.distributed.deduplicator.absorber.impl

import com.github.benmanes.caffeine.cache.Caffeine
import com.stroiker.distributed.deduplicator.absorber.DuplicateBurstAbsorber
import java.time.Duration
import java.util.concurrent.CompletableFuture

class CachedDuplicateBurstAbsorber(size: Long, ttl: Duration) : DuplicateBurstAbsorber {

    private val cache = Caffeine.newBuilder()
        .maximumSize(size)
        .expireAfterWrite(ttl)
        .buildAsync<String, String>()
        .asMap()

    override fun absorb(key: String, loader: () -> String): String {
        val future = CompletableFuture<String>()
        return cache.putIfAbsent(key, future)?.join() ?: loader().also { future.complete(it) }
    }

    override fun evict(key: String) {
        cache.compute(key) { _, _ -> null }
    }
}
