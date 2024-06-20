package com.stroiker.distributed.deduplicator

import kotlin.random.Random

fun randomString(length: Int = 32) = StringBuilder().apply {
    repeat(length) { append(Random.nextInt(0, 36).digitToChar(36)) }
}.toString()
