package com.eignex.kumulant.stream

import kotlin.test.Test
import kotlin.test.assertEquals

class MutexTest {

    @Test
    fun `withLock returns the block result`() {
        assertEquals(42, Mutex().withLock { 42 })
    }

    @Test
    fun `a non-concurrent mutex permits nested blocks`() {
        val mutex = Mutex(concurrent = false)
        assertEquals(7, mutex.withLock { mutex.withLock { 7 } })
    }
}
