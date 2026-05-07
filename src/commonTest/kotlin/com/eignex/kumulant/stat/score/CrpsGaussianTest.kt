package com.eignex.kumulant.stat.score

import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-12

/** Cross-check formula for the Gaussian CRPS (same identity as CrpsGaussian.kt). */
private fun referenceCrps(mean: Double, stdDev: Double, y: Double): Double {
    if (stdDev == 0.0) return abs(y - mean)
    val z = (y - mean) / stdDev
    val pdf = exp(-0.5 * z * z) / sqrt(2.0 * PI)
    val cdf = 0.5 * (1.0 + erf(z / sqrt(2.0)))
    return stdDev * (z * (2.0 * cdf - 1.0) + 2.0 * pdf - 1.0 / sqrt(PI))
}

private fun erf(x: Double): Double {
    val sign = if (x < 0.0) -1.0 else 1.0
    val ax = abs(x)
    val t = 1.0 / (1.0 + 0.3275911 * ax)
    val y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * exp(-ax * ax)
    return sign * y
}

class CrpsGaussianTest {

    @Test
    fun `running mean equals manually accumulated per-row CRPS`() {
        val rows = listOf(
            doubleArrayOf(0.0, 1.0, 0.5),
            doubleArrayOf(1.0, 2.0, -0.5),
            doubleArrayOf(-1.0, 0.5, 0.0),
        )
        val stat = CrpsGaussian()
        var sum = 0.0
        for (row in rows) {
            stat.update(row, timestampNanos = 0L, weight = 1.0)
            sum += referenceCrps(row[0], row[1], row[2])
        }
        val read = stat.read(0L)
        assertEquals(rows.size.toDouble(), read.totalWeights, DELTA)
        assertEquals(sum / rows.size, read.mean, DELTA)
    }

    @Test
    fun `wrong vector size throws`() {
        val stat = CrpsGaussian()
        assertFailsWith<IllegalArgumentException> {
            stat.update(doubleArrayOf(0.0, 1.0), timestampNanos = 0L, weight = 1.0)
        }
    }

    @Test
    fun `merge composes mean across two segments`() {
        val rows = listOf(
            doubleArrayOf(0.0, 1.0, 0.0),
            doubleArrayOf(0.0, 1.0, 1.0),
            doubleArrayOf(0.0, 1.0, -1.0),
            doubleArrayOf(0.0, 1.0, 2.0),
        )
        val a = CrpsGaussian().also {
            for (i in 0..1) it.update(rows[i], 0L, 1.0)
        }
        val b = CrpsGaussian().also {
            for (i in 2..3) it.update(rows[i], 0L, 1.0)
        }
        val merged = CrpsGaussian().also {
            for (i in 0..1) it.update(rows[i], 0L, 1.0)
            it.merge(b.read(0L))
        }
        val ref = CrpsGaussian().also {
            for (row in rows) it.update(row, 0L, 1.0)
        }
        assertEquals(ref.read(0L).mean, merged.read(0L).mean, DELTA)
        assertEquals(ref.read(0L).totalWeights, merged.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `reset clears accumulated state`() {
        val stat = CrpsGaussian()
        stat.update(doubleArrayOf(0.0, 1.0, 0.5), 0L, 1.0)
        stat.reset()
        assertEquals(0.0, stat.read(0L).totalWeights, DELTA)
    }
}
