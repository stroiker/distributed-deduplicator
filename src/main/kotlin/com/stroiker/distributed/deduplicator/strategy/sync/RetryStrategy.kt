package com.stroiker.distributed.deduplicator.strategy.sync

interface RetryStrategy {

    fun <T> retry(action: () -> T): T
}
