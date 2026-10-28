package com.eignex.kumulant.stat.rate

import kotlin.test.Test
import kotlin.test.assertEquals

class RateResultTest {

    @Test
    fun `rate preserves nanosecond intervals at large timestamps`() {
        for (start in listOf(
            Long.MIN_VALUE,
            -1_000_000_000_000_000_000L,
            1_000_000_000_000_000_000L,
            Long.MAX_VALUE - 1,
        )) {
            val result = RateResult(start, 1.0, start + 1)

            assertEquals(1_000_000_000.0, result.rate, 1e-6, "start=$start")
        }
    }
}
