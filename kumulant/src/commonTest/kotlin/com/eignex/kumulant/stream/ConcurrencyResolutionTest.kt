package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ConcurrencyResolutionTest {
    @Test
    fun `first writer election succeeds once under every concurrency level`() {
        for (level in Concurrency.entries) {
            val cell = level.firstWriterMode().newLong(0L)

            val first = cell.compareAndSet(0L, 1L)
            val second = cell.compareAndSet(0L, 2L)

            assertTrue(first, "level=$level")
            assertFalse(second, "level=$level")
        }
    }
}
