package com.eignex.kumulant.stat.anomaly

import com.eignex.kumulant.DELTA
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class GaussianScorerStatTest {

    @Test
    fun `score is zero on a constant stream`() {
        val s = GaussianScorerStat()
        repeat(10) { s.update(7.0) }
        val r = s.read()
        assertEquals(7.0, r.mean, DELTA)
        assertEquals(0.0, r.variance, DELTA)
        assertEquals(0.0, r.score(7.0), DELTA)
        assertEquals(0.0, r.score(100.0), DELTA) // variance floor protects against blow-up
    }

    @Test
    fun `score grows with deviation from the running mean`() {
        val s = GaussianScorerStat()
        val samples = doubleArrayOf(-1.0, -0.5, 0.0, 0.0, 0.5, 1.0)
        for (x in samples) s.update(x)
        val r = s.read()
        assertTrue(abs(r.mean) < 0.5, "mean=${r.mean}")
        assertTrue(r.stdDev > 0.0)
        val low = r.score(0.0)
        val high = r.score(5.0)
        assertTrue(high > low, "score should grow with |x - mean|: low=$low, high=$high")
    }

    @Test
    fun `merge folds two streams sample-weighted`() {
        val a = GaussianScorerStat()
        val b = GaussianScorerStat()
        for (x in doubleArrayOf(1.0, 2.0)) a.update(x)
        for (x in doubleArrayOf(3.0, 4.0)) b.update(x)
        a.merge(b.read())
        val r = a.read()
        assertEquals(4.0, r.totalWeights, DELTA)
        assertEquals(2.5, r.mean, DELTA)
    }
}
