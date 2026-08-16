package com.eignex.kumulant.math

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private const val DELTA = 1e-12

/**
 * Properties of the shared softmax, which three call sites previously each implemented for themselves.
 */
class SoftmaxTest {

    @Test
    fun `probabilities sum to one and preserve order`() {
        val p = doubleArrayOf(1.0, 3.0, 2.0, -5.0)

        assertTrue(p.softmaxInPlace())

        assertEquals(1.0, p.sum(), DELTA)
        assertTrue(p.all { it > 0.0 }, "every probability must be positive: ${p.toList()}")
        assertTrue(p[1] > p[2] && p[2] > p[0] && p[0] > p[3], "order was not preserved: ${p.toList()}")
    }

    @Test
    fun `a constant shift in the logits changes nothing`() {
        // The identity the max-subtraction relies on. If this ever stopped holding, the overflow
        // guard below would be changing the answer rather than just making it computable.
        val plain = doubleArrayOf(0.5, -1.25, 4.0)
        val shifted = DoubleArray(plain.size) { plain[it] + 137.0 }

        plain.softmaxInPlace()
        shifted.softmaxInPlace()

        for (i in plain.indices) {
            assertEquals(plain[i], shifted[i], DELTA, "coordinate $i moved under a constant shift")
        }
    }

    @Test
    fun `enormous logits do not overflow`() {
        // Without the shift every one of these is exp(inf) and the array comes back all-NaN. This is
        // the whole reason the loop is not two lines long.
        val p = doubleArrayOf(1000.0, 1001.0, 999.0)

        assertTrue(p.softmaxInPlace())

        assertEquals(1.0, p.sum(), DELTA)
        assertTrue(p.all { it.isFinite() }, "overflowed: ${p.toList()}")
        // The winner by one nat, so exactly the two-way logistic split against each rival.
        assertTrue(p[1] > 0.5, "the largest logit should dominate: ${p.toList()}")
    }

    @Test
    fun `tiny logits do not underflow to an empty distribution`() {
        // The mirror case. Every exponential here would be zero unshifted, leaving nothing to
        // normalise by; after the shift the largest is exp(0) and the sum cannot be zero.
        val p = doubleArrayOf(-1000.0, -1000.5, -1001.0)

        assertTrue(p.softmaxInPlace())

        assertEquals(1.0, p.sum(), DELTA)
        assertTrue(p.all { it > 0.0 }, "underflowed: ${p.toList()}")
    }

    @Test
    fun `a uniform input stays uniform`() {
        val p = DoubleArray(4) { 7.0 }

        assertTrue(p.softmaxInPlace())

        for (v in p) assertEquals(0.25, v, DELTA)
    }

    @Test
    fun `an empty array reports failure rather than dividing by zero`() {
        assertFalse(DoubleArray(0).softmaxInPlace())
    }

    @Test
    fun `a single class is certain`() {
        val p = doubleArrayOf(-42.0)

        assertTrue(p.softmaxInPlace())

        assertEquals(1.0, p[0], DELTA)
    }

    @Test
    fun `a NaN logit yields NaN rather than being rejected`() {
        // Pinned as the current behaviour, not as desirable behaviour. The post-shift sum is NaN,
        // which fails the `<= 0.0` guard the three previous copies all carried, so this returns true
        // with an unusable array - and on the training path that NaN reaches the weights and stays
        // there. Kept exactly as it was so this extraction changes nothing; the fix is its own task.
        val p = doubleArrayOf(1.0, Double.NaN, 2.0)

        assertTrue(p.softmaxInPlace(), "the sum guard cannot catch a NaN, so this still reports success")

        assertTrue(p.all { it.isNaN() }, "expected the NaN to spread: ${p.toList()}")
    }

    private fun DoubleArray.sum(): Double {
        var s = 0.0
        for (v in this) s += v
        return s
    }

    private fun DoubleArray.all(predicate: (Double) -> Boolean): Boolean {
        for (v in this) if (!predicate(v)) return false
        return true
    }

    @Test
    fun `the two-class case matches the logistic function`() {
        // Cross-check against a formula that does not share this implementation's shape.
        val p = doubleArrayOf(0.0, 2.0)

        p.softmaxInPlace()

        val logistic = 1.0 / (1.0 + kotlin.math.exp(-2.0))
        assertTrue(abs(p[1] - logistic) < DELTA, "expected $logistic, got ${p[1]}")
    }
}
