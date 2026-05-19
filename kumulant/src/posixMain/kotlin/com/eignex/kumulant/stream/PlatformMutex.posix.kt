@file:OptIn(ExperimentalForeignApi::class, ExperimentalNativeApi::class)
@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

import kotlinx.cinterop.Arena
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.ptr
import platform.posix.pthread_mutex_destroy
import platform.posix.pthread_mutex_init
import platform.posix.pthread_mutex_lock
import platform.posix.pthread_mutex_t
import platform.posix.pthread_mutex_unlock
import kotlin.experimental.ExperimentalNativeApi
import kotlin.native.ref.createCleaner

internal actual class PlatformMutex actual constructor() : Mutex {
    private val arena = Arena()
    private val mutex = arena.alloc<pthread_mutex_t>().also {
        pthread_mutex_init(it.ptr, null)
    }

    @Suppress("unused")
    private val cleaner = createCleaner(arena to mutex.ptr) { (a, p) ->
        pthread_mutex_destroy(p)
        a.clear()
    }

    actual override fun <R> withLock(block: () -> R): R {
        pthread_mutex_lock(mutex.ptr)
        try {
            return block()
        } finally {
            pthread_mutex_unlock(mutex.ptr)
        }
    }
}
