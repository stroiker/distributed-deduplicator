package com.stroiker.distributed.deduplicator.absorber.impl

import com.stroiker.distributed.deduplicator.randomString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class CachedDuplicateBurstAbsorberTest {

    private val absorber = CachedDuplicateBurstAbsorber(1, Duration.ofSeconds(60))

    @Test
    fun `should absorb and manually evict key`() {
        val key = randomString()
        val value = randomString()
        val loaderCounter = AtomicInteger()
        val loader = { loaderCounter.incrementAndGet(); value }
        assertEquals(value, absorber.absorb(key, loader))
        assertEquals(value, absorber.absorb(key, loader))
        absorber.evict(key)
        assertEquals(value, absorber.absorb(key, loader))
        assertEquals(2, loaderCounter.get())
    }
}
