package com.stroiker.distributed.deduplicator.absorber

interface DuplicateBurstAbsorber {

    fun absorb(key: String, loader: () -> String): String

    fun evict(key: String)
}
