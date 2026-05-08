@file:OptIn(ExperimentalForeignApi::class, ExperimentalNativeApi::class)
@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

import kotlinx.cinterop.Arena
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.ptr
import platform.windows.CRITICAL_SECTION
import platform.windows.DeleteCriticalSection
import platform.windows.EnterCriticalSection
import platform.windows.InitializeCriticalSection
import platform.windows.LeaveCriticalSection
import kotlin.experimental.ExperimentalNativeApi
import kotlin.native.ref.createCleaner

actual class PlatformMutex actual constructor() : Mutex {
    private val arena = Arena()
    private val cs = arena.alloc<CRITICAL_SECTION>().also {
        InitializeCriticalSection(it.ptr)
    }

    @Suppress("unused")
    private val cleaner = createCleaner(arena to cs.ptr) { (a, p) ->
        DeleteCriticalSection(p)
        a.clear()
    }

    actual override fun <R> withLock(block: () -> R): R {
        EnterCriticalSection(cs.ptr)
        try {
            return block()
        } finally {
            LeaveCriticalSection(cs.ptr)
        }
    }
}
