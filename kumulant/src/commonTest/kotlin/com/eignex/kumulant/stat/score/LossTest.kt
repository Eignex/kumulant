package com.eignex.kumulant.stat.score

import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class MseLossTest {

    @Test
    fun `mean of squared residuals`() {
        val stat = MseLossStat().apply {
            update(2.0, 1.0, 0L, 1.0)
            update(3.0, 1.0, 0L, 1.0)
            update(0.0, 0.0, 0L, 1.0)
        }
        assertEquals(5.0 / 3.0, stat.read(0L).mean, DELTA)
    }

    @Test
    fun `weighted updates use Welford weighted mean`() {
        val stat = MseLossStat().apply {
            update(2.0, 0.0, 0L, 2.0)
            update(0.0, 0.0, 0L, 1.0)
        }
        assertEquals(8.0 / 3.0, stat.read(0L).mean, DELTA)
        assertEquals(3.0, stat.read(0L).totalWeights, DELTA)
    }
}

class MaeLossTest {

    @Test
    fun `mean of absolute residuals`() {
        val stat = MaeLossStat().apply {
            update(2.0, 1.0, 0L, 1.0)
            update(-1.0, 1.0, 0L, 1.0)
            update(0.0, 0.0, 0L, 1.0)
        }
        assertEquals(1.0, stat.read(0L).mean, DELTA)
    }
}

class LogLossTest {

    @Test
    fun `confident-correct loss equals minus ln of probability`() {
        val stat = LogLossStat().apply { update(x = 0.9, y = 1.0, timestampNanos = 0L, weight = 1.0) }
        assertEquals(-ln(0.9), stat.read(0L).mean, DELTA)
    }

    @Test
    fun `confident-wrong predictions are clamped not infinite`() {
        val stat = LogLossStat().apply { update(x = 0.0, y = 1.0, timestampNanos = 0L, weight = 1.0) }
        val mean = stat.read(0L).mean
        assertEquals(true, mean.isFinite())
        // Clamped to eps=1e-15, so loss ~ -ln(1e-15) ~ 34.5
        assertEquals(-ln(1e-15), mean, 1e-9)
    }

    @Test
    fun `mean over balanced predictions`() {
        val stat = LogLossStat().apply {
            update(0.9, 1.0, 0L, 1.0)
            update(0.1, 0.0, 0L, 1.0)
        }
        val expected = (-ln(0.9) - ln(0.9)) / 2.0
        assertEquals(expected, stat.read(0L).mean, DELTA)
    }
}
