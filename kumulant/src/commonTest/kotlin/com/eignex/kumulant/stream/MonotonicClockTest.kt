package com.eignex.kumulant.stream

import kotlin.test.Test
import kotlin.test.assertTrue

class MonotonicClockTest {

    @Test
    fun `currentTimeNanos never decreases across successive calls`() {
        val samples = LongArray(1_000) { currentTimeNanos() }
        for (i in 1 until samples.size) {
            assertTrue(
                samples[i] >= samples[i - 1],
                "sample[$i]=${samples[i]} < sample[${i - 1}]=${samples[i - 1]}",
            )
        }
    }
}
