package com.stroiker.distributed.deduplicator.strategy.sync.impl

import com.stroiker.distributed.deduplicator.exception.RetriesExceededException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.strategy.sync.RetryStrategy

class NoRetryStrategy : RetryStrategy {

    override fun <T> retry(action: () -> T): T =
        runCatching { action() }
            .getOrElse { error ->
                when (error) {
                    is RetryException -> throw RetriesExceededException(error.key, error.table)
                    else -> throw error
                }
            }
}
