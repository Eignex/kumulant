package com.eignex.kumulant.concurrent

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class HashingTest {

    @Test
    fun `splitmix64 matches reference vectors`() {
        // Locked-in outputs — guard against accidental edits to the mixer constants
        // in Hashing.kt.
        assertEquals(4553024054441242788L, splitmix64(0L))
        assertEquals(6615013642232309006L, splitmix64(1L))
        assertEquals(-2854182258122887073L, splitmix64(-1L))
        assertEquals(189591720424502296L, splitmix64(42L))
        assertEquals(2950148240053738312L, splitmix64(Long.MAX_VALUE))
        assertEquals(-6357312718828134429L, splitmix64(Long.MIN_VALUE))
    }

    @Test
    fun `splitmix64 produces approximately balanced bits across sequential inputs`() {
        // Avalanche smoke test: each output bit should be set ~50% of the time over a
        // large run of sequential inputs. Allow a generous 5% tolerance.
        val samples = 10_000
        val counts = IntArray(64)
        for (i in 0 until samples) {
            val h = splitmix64(i.toLong())
            for (b in 0 until 64) {
                if ((h ushr b) and 1L == 1L) counts[b]++
            }
        }
        for (b in 0 until 64) {
            val rate = counts[b].toDouble() / samples
            assertTrue(abs(rate - 0.5) < 0.05, "bit $b rate=$rate")
        }
    }
}
