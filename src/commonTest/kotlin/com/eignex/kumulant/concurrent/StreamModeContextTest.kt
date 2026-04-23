package com.eignex.kumulant.concurrent

import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class StreamModeContextTest {

    // defaultStreamMode is a global — snapshot and restore so tests don't pollute each other.
    private val snapshotMode = defaultStreamMode

    @AfterTest
    fun restore() {
        defaultStreamMode = snapshotMode
    }

    @Test
    fun `withMode restores previous mode after block returns`() {
        defaultStreamMode = SerialMode
        withMode(AtomicMode) {
            assertSame(AtomicMode, defaultStreamMode)
        }
        assertSame(SerialMode, defaultStreamMode)
    }

    @Test
    fun `withMode restores previous mode when block throws`() {
        defaultStreamMode = SerialMode
        try {
            withMode(AtomicMode) {
                throw RuntimeException("boom")
            }
        } catch (_: RuntimeException) {
            // expected
        }
        assertSame(SerialMode, defaultStreamMode)
    }

    @Test
    fun `withMode returns the block's value`() {
        val result = withMode(AtomicMode) { 42 }
        assertEquals(42, result)
    }

    @Test
    fun `withMode nests correctly`() {
        defaultStreamMode = SerialMode
        withMode(AtomicMode) {
            assertSame(AtomicMode, defaultStreamMode)
            val fixed = FixedAtomicMode(precision = 2)
            withMode(fixed) {
                assertSame(fixed, defaultStreamMode)
            }
            assertSame(AtomicMode, defaultStreamMode)
        }
        assertSame(SerialMode, defaultStreamMode)
    }
}
