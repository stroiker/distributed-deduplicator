package com.stroiker.distributed.deduplicator.exception

class RetryException(val key: String, val table: String) : RuntimeException(String.format(MESSAGE, key, table)) {

    companion object {
        const val MESSAGE = "Deduplication key '%s' in table '%s' is marked as RETRY"
    }
}
