package com.eignex.kumulant.stat.score

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

/** Re-implements the ensemble CRPS estimator for cross-checking the stat output. */
private fun referenceCrps(samples: DoubleArray, y: Double): Double {
    val sorted = samples.copyOf().also { it.sort() }
    val m = sorted.size
    var meanAbs = 0.0
    for (x in sorted) meanAbs += abs(x - y)
    meanAbs /= m
    var pairSum = 0.0
    for (k in 0 until m - 1) {
        pairSum += (k + 1).toLong() * (m - k - 1).toLong() * (sorted[k + 1] - sorted[k])
    }
    return meanAbs - pairSum / (m.toLong() * m)
}

class CrpsEnsembleTest {

    @Test
    fun `mean equals manually accumulated per row crps`() {
        val rows = listOf(
            doubleArrayOf(1.0, 2.0, 3.0) to 2.0,
            doubleArrayOf(0.0, 1.0) to 1.5,
            doubleArrayOf(5.0, 6.0, 7.0, 8.0) to 5.0,
        )
        val stat = CrpsEnsemble()
        var sum = 0.0
        for ((samples, y) in rows) {
            stat.update(samples, y)
            sum += referenceCrps(samples, y)
        }
        val r = stat.read(0L)
        assertEquals(rows.size.toDouble(), r.totalWeights, DELTA)
        assertEquals(sum / rows.size, r.mean, DELTA)
    }

    @Test
    fun `weighted updates aggregate via Welford weights`() {
        val a = doubleArrayOf(1.0, 2.0, 3.0)
        val b = doubleArrayOf(10.0, 20.0)
        val stat = CrpsEnsemble().apply {
            update(a, 2.0, weight = 2.0)
            update(b, 15.0, weight = 1.0)
        }
        val expected = (referenceCrps(a, 2.0) * 2.0 + referenceCrps(b, 15.0) * 1.0) / 3.0
        assertEquals(expected, stat.read(0L).mean, DELTA)
        assertEquals(3.0, stat.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `merge composes mean across two segments`() {
        val f = doubleArrayOf(0.0, 1.0)
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
            update(doubleArrayOf(1.0, 2.0), 1.5)
            reset()
        }
        assertEquals(0.0, stat.read(0L).totalWeights, DELTA)
    }
}
