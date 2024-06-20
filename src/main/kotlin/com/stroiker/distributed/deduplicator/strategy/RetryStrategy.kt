package com.stroiker.distributed.deduplicator.strategy

interface RetryStrategy {

    fun <T> retry(action: () -> T): T
}
