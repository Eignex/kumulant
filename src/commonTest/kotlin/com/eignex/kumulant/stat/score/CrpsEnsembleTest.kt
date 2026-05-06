package com.eignex.kumulant.stat.score

import com.eignex.kumulant.forecast.EnsembleForecast
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class CrpsEnsembleTest {

    @Test
    fun `mean equals manually accumulated per row crps`() {
        val rows = listOf(
            EnsembleForecast(doubleArrayOf(1.0, 2.0, 3.0)) to 2.0,
            EnsembleForecast(doubleArrayOf(0.0, 1.0)) to 1.5,
            EnsembleForecast(doubleArrayOf(5.0, 6.0, 7.0, 8.0)) to 5.0,
        )
        val stat = CrpsEnsemble()
        var sum = 0.0
        for ((forecast, y) in rows) {
            stat.update(forecast, y)
            sum += forecast.crps(y)
        }
        val r = stat.read(0L)
        assertEquals(rows.size.toDouble(), r.totalWeights, DELTA)
        assertEquals(sum / rows.size, r.mean, DELTA)
    }

    @Test
    fun `weighted updates aggregate via Welford weights`() {
        val a = EnsembleForecast(doubleArrayOf(1.0, 2.0, 3.0))
        val b = EnsembleForecast(doubleArrayOf(10.0, 20.0))
        val stat = CrpsEnsemble().apply {
            update(a, 2.0, weight = 2.0)
            update(b, 15.0, weight = 1.0)
        }
        val expected = (a.crps(2.0) * 2.0 + b.crps(15.0) * 1.0) / 3.0
        assertEquals(expected, stat.read(0L).mean, DELTA)
        assertEquals(3.0, stat.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `merge composes mean across two segments`() {
        val f = EnsembleForecast(doubleArrayOf(0.0, 1.0))
        val a = CrpsEnsemble().apply {
            update(f, 0.5)
            update(f, 0.7)
        }
        val b = CrpsEnsemble().apply {
            update(f, 0.2)
            update(f, 0.9)
        }
        val ref = CrpsEnsemble().apply {
            update(f, 0.5)
            update(f, 0.7)
            update(f, 0.2)
            update(f, 0.9)
        }
        a.merge(b.read(0L))
        assertEquals(ref.read(0L).mean, a.read(0L).mean, DELTA)
        assertEquals(ref.read(0L).totalWeights, a.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val stat = CrpsEnsemble().apply {
            update(EnsembleForecast(doubleArrayOf(1.0, 2.0)), 1.5)
            reset()
        }
        assertEquals(0.0, stat.read(0L).totalWeights, DELTA)
    }
}
