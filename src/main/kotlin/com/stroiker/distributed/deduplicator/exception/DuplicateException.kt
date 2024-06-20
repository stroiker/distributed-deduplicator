package com.stroiker.distributed.deduplicator.exception

class DuplicateException(key: String, table: String) : RuntimeException(String.format(MESSAGE, key, table)) {

    companion object {
        const val MESSAGE = "Deduplication key '%s' in table '%s' is marked as DUPLICATE"
    }
}
