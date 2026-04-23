package com.eignex.kumulant.stat

import com.eignex.kumulant.core.SketchResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class RangeEdgeCasesTest {

    @Test
    fun `positive infinity sets max`() {
        val r = Range()
        r.update(1.0)
        r.update(Double.POSITIVE_INFINITY)
        val result = r.read()
        assertEquals(1.0, result.min, DELTA)
        assertTrue(result.max.isInfinite() && result.max > 0.0)
    }

    @Test
    fun `negative infinity sets min`() {
        val r = Range()
        r.update(1.0)
        r.update(Double.NEGATIVE_INFINITY)
        val result = r.read()
        assertTrue(result.min.isInfinite() && result.min < 0.0)
        assertEquals(1.0, result.max, DELTA)
    }

    @Test
    fun `NaN is ignored by the less-than and greater-than comparisons`() {
        // All comparisons with NaN return false, so NaN inputs leave min/max untouched.
        val r = Range()
        r.update(5.0)
        r.update(Double.NaN)
        val result = r.read()
        assertEquals(5.0, result.min, DELTA)
        assertEquals(5.0, result.max, DELTA)
    }

    @Test
    fun `read before any update returns infinities`() {
        val result = Range().read()
        assertTrue(result.min.isInfinite() && result.min > 0.0)
        assertTrue(result.max.isInfinite() && result.max < 0.0)
    }
}

class MinMaxEdgeCasesTest {

    @Test
    fun `Min ignores NaN inputs`() {
        val m = Min()
        m.update(5.0)
        m.update(Double.NaN)
        assertEquals(5.0, m.read().min, DELTA)
    }

    @Test
    fun `Max ignores NaN inputs`() {
        val m = Max()
        m.update(5.0)
        m.update(Double.NaN)
        assertEquals(5.0, m.read().max, DELTA)
    }

    @Test
    fun `Min accepts negative infinity`() {
        val m = Min()
        m.update(0.0)
        m.update(Double.NEGATIVE_INFINITY)
        assertTrue(m.read().min.isInfinite() && m.read().min < 0.0)
    }

    @Test
    fun `Max accepts positive infinity`() {
        val m = Max()
        m.update(0.0)
        m.update(Double.POSITIVE_INFINITY)
        assertTrue(m.read().max.isInfinite() && m.read().max > 0.0)
    }
}

class VarianceEdgeCasesTest {

    @Test
    fun `read before any update returns zero variance and zero mean`() {
        val v = Variance().read()
        assertEquals(0.0, v.totalWeights, DELTA)
        assertEquals(0.0, v.mean, DELTA)
        assertEquals(0.0, v.variance, DELTA)
    }

    @Test
    fun `variance over constant stream is zero`() {
        val v = Variance()
        repeat(100) { v.update(7.0) }
        assertEquals(0.0, v.read().variance, 1e-6)
    }

    @Test
    fun `handles large magnitudes without overflow`() {
        val v = Variance()
        val large = 1e9
        v.update(large)
        v.update(-large)
        val result = v.read()
        assertFalse(result.variance.isNaN())
        assertFalse(result.variance.isInfinite())
        // True variance of { +1e9, -1e9 } with mean 0 is (1e18 + 1e18)/2 = 1e18.
        assertEquals(1e18, result.variance, 1e12)
    }
}

class EwmaEdgeCasesTest {

    @Test
    fun `EwmaMean before any update returns zero total weight and zero mean`() {
        val m = EwmaMean(alpha = 0.1)
        val r = m.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.mean, DELTA)
    }

    @Test
    fun `EwmaVariance before any update returns zero state`() {
        val v = EwmaVariance(alpha = 0.1)
        val r = v.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.variance, DELTA)
    }
}

class DDSketchEdgeCasesTest {

    @Test
    fun `merge with incompatible relative error throws`() {
        val a = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        a.update(10.0)

        val b = DDSketch(relativeError = 0.05, probabilities = doubleArrayOf(0.5))
        b.update(20.0)

        assertFailsWith<IllegalArgumentException> {
            a.merge(b.read())
        }
    }

    @Test
    fun `merge with identical relative error succeeds`() {
        val a = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        a.update(10.0)

        val b = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        b.update(20.0)

        a.merge(b.read())
        assertEquals(2.0, a.read().totalWeights, DELTA)
    }

    @Test
    fun `merge with empty sketch is a no-op on existing data`() {
        val a = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        repeat(10) { a.update(5.0) }
        val before = a.read()

        // An "empty" result with matching gamma.
        a.merge(
            SketchResult(
                probabilities = before.probabilities,
                quantiles = DoubleArray(before.probabilities.size),
                gamma = before.gamma,
                totalWeights = 0.0,
                zeroCount = 0.0,
                positiveBins = emptyMap(),
                negativeBins = emptyMap(),
            )
        )

        assertEquals(10.0, a.read().totalWeights, DELTA)
    }
}
