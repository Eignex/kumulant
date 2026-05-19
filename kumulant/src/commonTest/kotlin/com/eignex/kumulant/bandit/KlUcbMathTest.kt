package com.eignex.kumulant.bandit

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KlUcbMathTest {

    @Test
    fun `klBernoulli is zero for matching interior means`() {
        // Boundary q values (0 or 1) return +infinity by convention since the log diverges.
        assertEquals(0.0, KlUcb.klBernoulli(0.5, 0.5), 1e-12)
        assertEquals(0.0, KlUcb.klBernoulli(0.3, 0.3), 1e-12)
        assertEquals(0.0, KlUcb.klBernoulli(0.7, 0.7), 1e-12)
    }

    @Test
    fun `klBernoulli handles boundary p values`() {
        // KL(0, q) = -ln(1 - q)
        assertEquals(-kotlin.math.ln(1.0 - 0.3), KlUcb.klBernoulli(0.0, 0.3), 1e-12)
        // KL(1, q) = -ln(q)
        assertEquals(-kotlin.math.ln(0.4), KlUcb.klBernoulli(1.0, 0.4), 1e-12)
    }

    @Test
    fun `klBernoulli is infinite when q hits zero or one and p disagrees`() {
        assertTrue(KlUcb.klBernoulli(0.5, 0.0).isInfinite())
        assertTrue(KlUcb.klBernoulli(0.5, 1.0).isInfinite())
    }

    @Test
    fun `klBernoulliUpper collapses to p when bound is zero or negative`() {
        assertEquals(0.5, KlUcb.klBernoulliUpper(0.5, bound = 0.0, tol = 1e-6))
        assertEquals(0.5, KlUcb.klBernoulliUpper(0.5, bound = -0.1, tol = 1e-6))
    }

    @Test
    fun `klBernoulliUpper grows monotonically with the bound`() {
        val low = KlUcb.klBernoulliUpper(0.5, bound = 0.05, tol = 1e-6)
        val mid = KlUcb.klBernoulliUpper(0.5, bound = 0.2, tol = 1e-6)
        val high = KlUcb.klBernoulliUpper(0.5, bound = 1.0, tol = 1e-6)
        assertTrue(low < mid)
        assertTrue(mid < high)
        assertTrue(high <= 1.0)
    }

    @Test
    fun `klBernoulliUpper stays in zero one`() {
        val q = KlUcb.klBernoulliUpper(0.99, bound = 5.0, tol = 1e-6)
        assertTrue(q in 0.99..1.0, "q=$q")
    }
}
