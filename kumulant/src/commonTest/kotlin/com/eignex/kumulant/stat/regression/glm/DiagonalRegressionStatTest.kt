package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.core.RegressionStat
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DiagonalRegressionStatTest {

    private fun fitLine(
        stat: RegressionStat<*>,
        slope: DoubleArray,
        intercept: Double,
        n: Int = 4000,
        seed: Long = 42L,
    ) {
        val rng = Random(seed)
        repeat(n) {
            val x = DoubleArray(slope.size) { rng.nextDouble() * 2.0 - 1.0 }
            var y = intercept
            for (i in slope.indices) y += slope[i] * x[i]
            y += rng.nextDouble() * 0.02 - 0.01 // small noise
            stat.update(x, y, 1.0)
        }
    }

    @Test
    fun `diagonal should recover ground truth weights with finite per-coefficient precision`() {
        val stat = DiagonalRegressionStat(featureSize = 3, priorPrecision = 0.01)
        val truth = doubleArrayOf(1.0, -1.5, 2.0)
        fitLine(stat, truth, intercept = -0.2)
        val r = stat.read()
        for (i in truth.indices) {
            assertTrue(
                abs(r.weights[i] - truth[i]) < 0.05,
                "weight[$i]=${r.weights[i]} far from truth=${truth[i]}",
            )
        }
        val precisionArr = r.precision.toDoubleArray()
        assertTrue(precisionArr.all { it > 100.0 }, "precision should grow with data: ${precisionArr.toList()}")
        assertTrue(r.biasPrecision > 100.0)
    }

    @Test
    fun `merge on diagonal regression combines independent normals`() {
        val a = DiagonalRegressionStat(featureSize = 2, priorPrecision = 0.01)
        val b = DiagonalRegressionStat(featureSize = 2, priorPrecision = 0.01)
        val truth = doubleArrayOf(2.0, -1.0)
        fitLine(a, truth, intercept = 0.0, n = 2000, seed = 1L)
        fitLine(b, truth, intercept = 0.0, n = 2000, seed = 2L)
        a.merge(b.read())
        val r = a.read()
        assertEquals(4000.0, r.totalWeights, absoluteTolerance = 1e-9)
        for (i in truth.indices) assertTrue(abs(r.weights[i] - truth[i]) < 0.05)
    }

    @Test
    fun `reset restores prior state`() {
        val stat = DiagonalRegressionStat(featureSize = 2, priorPrecision = 0.5)
        fitLine(stat, doubleArrayOf(1.0, 1.0), intercept = 0.0, n = 100)
        stat.reset()
        val r = stat.read()
        assertEquals(0.0, r.bias)
        assertEquals(0.0, r.totalWeights)
        assertEquals(0L, r.step)
        assertTrue(r.weights.toDoubleArray().all { it == 0.0 })
        assertTrue(r.precision.toDoubleArray().all { it == 0.5 })
    }

    @Test
    fun `Diagonal with Logit link tightens precision via curvature`() {
        val rng = Random(5)
        val stat = DiagonalRegressionStat(featureSize = 2, link = Link.Logit, priorPrecision = 0.5)
        repeat(500) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
            val y = if (rng.nextDouble() < 0.5) 1.0 else 0.0
            stat.update(x, y, 1.0)
        }
        val snap = stat.read()
        assertTrue(snap.precision[0] > 0.5, "precision[0] = ${snap.precision[0]}")
        assertTrue(snap.precision[1] > 0.5, "precision[1] = ${snap.precision[1]}")
    }
}
