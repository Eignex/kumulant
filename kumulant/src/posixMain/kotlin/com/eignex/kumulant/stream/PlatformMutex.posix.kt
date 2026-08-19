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
import platform.posix.pthread_mutexattr_destroy
import platform.posix.pthread_mutexattr_init
import platform.posix.pthread_mutexattr_settype
import platform.posix.pthread_mutexattr_t
import kotlin.experimental.ExperimentalNativeApi
import kotlin.native.ref.createCleaner

internal actual class PlatformMutex actual constructor() : Mutex {
    private val arena = Arena()

    // Error-checking rather than the default type. A thread that re-acquires a lock it already holds
    // is a defect either way, but a default mutex answers it by hanging with no stack to read, on the
    // only targets that do not tolerate it. This turns that into a thrown error naming the mistake.
    private val attr = arena.alloc<pthread_mutexattr_t>().also {
        pthread_mutexattr_init(it.ptr)
        pthread_mutexattr_settype(it.ptr, ERRORCHECK_MUTEX_TYPE)
    }

    private val mutex = arena.alloc<pthread_mutex_t>().also {
        pthread_mutex_init(it.ptr, attr.ptr)
    }

    @Suppress("unused")
    private val cleaner = createCleaner(Triple(arena, mutex.ptr, attr.ptr)) { (a, m, at) ->
        pthread_mutex_destroy(m)
        pthread_mutexattr_destroy(at)
        a.clear()
    }

    actual override fun <R> withLock(block: () -> R): R {
        enter()
        try {
            return block()
        } finally {
            exit()
        }
    }

    actual fun enter() {
        val code = pthread_mutex_lock(mutex.ptr)
        check(code == 0) { reentryMessage(code) }
    }

    actual fun exit() {
        pthread_mutex_unlock(mutex.ptr)
    }

    private fun reentryMessage(code: Int): String =
        "pthread_mutex_lock failed with $code; a non-zero code here is a thread re-acquiring a lock it " +
            "already holds, which this lock does not allow - see PlatformMutex"
}
