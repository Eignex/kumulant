package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MomentsStatTest {

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = MomentsStat().apply {
            update(1.0)
            update(2.0)
            update(3.0)
        }
        val m2 = m1.create()
        m1.update(4.0)
        assertEquals(4.0, m1.read().totalWeights, DELTA)
        assertEquals(0.0, m2.read().totalWeights, DELTA)
    }

    @Test
    fun `test skewness for symmetric distribution`() {
        val stat = MomentsStat()

        val data = listOf(1.0, 2.0, 3.0, 4.0, 5.0)
        data.forEach { stat.update(it, 1.0) }

        assertEquals(3.0, stat.read().mean, DELTA)
        assertEquals(0.0, stat.read().skewness, DELTA)
    }

    @Test
    fun `test positive skewness`() {
        val stat = MomentsStat()

        val data = listOf(1.0, 1.0, 1.0, 2.0, 10.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.read().skewness > 0.0)
    }

    @Test
    fun `test negative skewness`() {
        val stat = MomentsStat()

        val data = listOf(10.0, 10.0, 10.0, 9.0, 1.0)
        data.forEach { stat.update(it, 1.0) }
        assertTrue(stat.read().skewness < 0.0)
    }

    @Test
    fun `test kurtosis of normal-ish distribution`() {
        val stat = MomentsStat()
        val data = listOf(-2.0, -1.0, 0.0, 1.0, 2.0)
        data.forEach { stat.update(it, 1.0) }

        assertTrue(stat.read().kurtosis < 0.0)
    }

    @Test
    fun `test leptokurtic distribution`() {
        val stat = MomentsStat()

        repeat(100) { stat.update(0.0, 1.0) }
        stat.update(100.0, 1.0)
        stat.update(-100.0, 1.0)
        assertTrue(stat.read().kurtosis > 0.0)
    }

    @Test
    fun `test complex merge`() {
        val m1 =
            MomentsStat().apply { listOf(10.0, 12.0).forEach { update(it, 1.0) } }
        val m2 =
            MomentsStat().apply { listOf(100.0, 120.0).forEach { update(it, 1.0) } }

        m1.merge(m2.read())

        assertEquals(60.5, m1.read().mean, DELTA)
        assertEquals(4.0, m1.read().totalWeights, DELTA)
    }

    @Test
    fun `test reset`() {
        val stat = MomentsStat()
        stat.update(10.0)
        stat.update(20.0)
        stat.reset()

        assertEquals(0.0, stat.read().totalWeights, DELTA)
        assertEquals(0.0, stat.read().mean, DELTA)
        assertEquals(0.0, stat.read().variance, DELTA)
        assertEquals(0.0, stat.read().skewness, DELTA)
        assertEquals(0.0, stat.read().kurtosis, DELTA)
    }

    @Test
    fun `sampleVariance applies Bessel correction`() {
        val stat = MomentsStat()
        listOf(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        assertEquals(5.0, r.mean, DELTA)
        assertEquals(4.0, r.variance, DELTA)
        assertEquals(32.0 / 7.0, r.sampleVariance, DELTA)
        assertEquals(sqrt(32.0 / 7.0), r.sampleStdDev, DELTA)
    }

    @Test
    fun `sampleVariance is zero when totalWeights le 1`() {
        val empty = MomentsStat().read()
        assertEquals(0.0, empty.sampleVariance, DELTA)
        assertEquals(0.0, empty.sampleStdDev, DELTA)

        val one = MomentsStat().apply { update(42.0, 1.0) }.read()
        assertEquals(0.0, one.sampleVariance, DELTA)
        assertEquals(0.0, one.sampleStdDev, DELTA)
    }

    @Test
    fun `unbiasedSkewness is zero with two or fewer samples`() {
        val empty = MomentsStat().read()
        assertEquals(0.0, empty.unbiasedSkewness, DELTA)

        val two = MomentsStat().apply {
            update(1.0, 1.0)
            update(3.0, 1.0)
        }.read()
        assertEquals(0.0, two.unbiasedSkewness, DELTA)
    }

    @Test
    fun `unbiasedSkewness scales biased skewness by sample-size factor`() {
        val stat = MomentsStat()
        listOf(1.0, 1.0, 1.0, 2.0, 10.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        val n = r.totalWeights
        val expected = (sqrt(n * (n - 1)) / (n - 2)) * r.skewness
        assertEquals(expected, r.unbiasedSkewness, DELTA)
        assertTrue(r.unbiasedSkewness > r.skewness)
    }

    @Test
    fun `unbiasedKurtosis is zero with three or fewer samples`() {
        val three = MomentsStat().apply {
            update(1.0, 1.0)
            update(2.0, 1.0)
            update(3.0, 1.0)
        }.read()
        assertEquals(0.0, three.unbiasedKurtosis, DELTA)
    }

    @Test
    fun `unbiasedKurtosis matches the algebraic definition`() {
        val stat = MomentsStat()
        listOf(-2.0, -1.0, 0.0, 1.0, 2.0, 3.0).forEach { stat.update(it, 1.0) }
        val r = stat.read()
        val n = r.totalWeights
        val expected = ((n - 1) / ((n - 2) * (n - 3))) * ((n + 1) * r.kurtosis + 6.0)
        assertEquals(expected, r.unbiasedKurtosis, DELTA)
    }

    /** Weighted central moments computed directly, as the reference for the recurrences. */
    private fun reference(points: List<Pair<Double, Double>>): MomentsResult {
        val w = points.sumOf { it.second }
        val mean = points.sumOf { it.first * it.second } / w
        fun central(k: Int) = points.sumOf { (x, wi) ->
            var acc = wi
            repeat(k) { acc *= (x - mean) }
            acc
        }
        return MomentsResult(w, mean, central(2), central(3), central(4))
    }

    private fun assertMomentsEqual(expected: MomentsResult, actual: MomentsResult, tolerance: Double = DELTA) {
        assertEquals(expected.totalWeights, actual.totalWeights, tolerance, "totalWeights")
        assertEquals(expected.mean, actual.mean, tolerance, "mean")
        assertEquals(expected.m2, actual.m2, tolerance, "m2")
        assertEquals(expected.m3, actual.m3, tolerance, "m3")
        assertEquals(expected.m4, actual.m4, tolerance, "m4")
    }

    @Test
    fun `higher moments match direct sums under a constant non-unit weight`() {
        val values = listOf(1.0, 2.0, 4.0, 8.0, 3.0)
        val stat = MomentsStat()
        values.forEach { stat.update(it, 2.0) }
        assertMomentsEqual(reference(values.map { it to 2.0 }), stat.read())
    }

    @Test
    fun `higher moments match direct sums under mixed weights`() {
        val points = listOf(1.0 to 0.5, 2.0 to 0.25, 4.0 to 3.0, 8.0 to 1.5, 3.0 to 0.75)
        val stat = MomentsStat()
        points.forEach { (x, w) -> stat.update(x, w) }
        assertMomentsEqual(reference(points), stat.read())
    }

    @Test
    fun `an integer weight equals that many unit-weight updates`() {
        val weighted = MomentsStat().apply {
            update(1.0, 3.0)
            update(5.0, 2.0)
            update(9.0, 1.0)
        }
        val repeated = MomentsStat().apply {
            repeat(3) { update(1.0, 1.0) }
            repeat(2) { update(5.0, 1.0) }
            update(9.0, 1.0)
        }
        assertMomentsEqual(repeated.read(), weighted.read())
    }

    @Test
    fun `skewness is scale-invariant under a uniform weight rescale`() {
        val values = listOf(1.0, 1.0, 1.0, 2.0, 10.0)
        val unit = MomentsStat().apply { values.forEach { update(it, 1.0) } }.read()
        val scaled = MomentsStat().apply { values.forEach { update(it, 7.0) } }.read()
        assertEquals(unit.skewness, scaled.skewness, DELTA)
        assertEquals(unit.kurtosis, scaled.kurtosis, DELTA)
    }

    @Test
    fun `negative weight downdates all four moments back to the prior state`() {
        val stat = MomentsStat()
        listOf(1.0 to 2.0, 5.0 to 0.75, 9.0 to 1.5).forEach { (x, w) -> stat.update(x, w) }
        val before = stat.read()

        stat.update(4.0, 2.25)
        stat.update(4.0, -2.25)

        assertMomentsEqual(before, stat.read(), tolerance = 1e-7)
    }

    @Test
    fun `downdate matches a fresh accumulator over the retained values`() {
        val windowed = MomentsStat()
        val all = listOf(2.0 to 1.0, 4.0 to 1.0, 6.0 to 1.0, 8.0 to 1.0)
        all.forEach { (x, w) -> windowed.update(x, w) }
        windowed.update(2.0, -1.0)

        assertMomentsEqual(reference(all.drop(1)), windowed.read(), tolerance = 1e-7)
    }

    @Test
    fun `a downdate that would exhaust the accumulated weight throws`() {
        val stat = MomentsStat()
        stat.update(1.0, 1.0)
        val before = stat.read()
        assertFailsWith<IllegalArgumentException> { stat.update(2.0, -1.0) }
        assertMomentsEqual(before, stat.read())
    }

    @Test
    fun `merge matches a single weighted pass over higher moments`() {
        val left = listOf(10.0 to 2.0, 12.0 to 0.5, 11.0 to 1.25)
        val right = listOf(100.0 to 3.0, 120.0 to 0.75)

        val merged = MomentsStat().apply { left.forEach { (x, w) -> update(x, w) } }
        merged.merge(MomentsStat().apply { right.forEach { (x, w) -> update(x, w) } }.read())

        assertMomentsEqual(reference(left + right), merged.read(), tolerance = 1e-7)
    }
}
