package com.stroiker.distributed.deduplicator.exception

class RetriesExceededException(key: String, table: String) : RuntimeException(String.format(MESSAGE, key, table)) {

    companion object {
        const val MESSAGE = "Retries of key '%s' in table '%s' are exceeded"
    }
}
