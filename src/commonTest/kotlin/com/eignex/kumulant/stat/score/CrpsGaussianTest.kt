package com.eignex.kumulant.stat.score

import com.eignex.kumulant.forecast.GaussianForecast
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-12

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
            sum += GaussianForecast(row[0], row[1]).crps(row[2])
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
