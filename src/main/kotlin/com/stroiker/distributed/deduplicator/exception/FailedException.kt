package com.stroiker.distributed.deduplicator.exception

class FailedException(key: String, table: String, reason: String?) :
    RuntimeException(String.format(MESSAGE, key, table, reason)) {

    companion object {
        const val MESSAGE = "Deduplication key '%s' request in table '%s' failed by reason: %s"
    }
}
