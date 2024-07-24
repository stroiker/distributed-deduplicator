package com.stroiker.distributed.deduplicator.absorber.impl

import com.stroiker.distributed.deduplicator.absorber.DuplicateBurstAbsorber

class NoDuplicateBurstAbsorber : DuplicateBurstAbsorber {
    override fun absorb(key: String, loader: () -> String): String = loader()
    override fun evict(key: String) {}
}
