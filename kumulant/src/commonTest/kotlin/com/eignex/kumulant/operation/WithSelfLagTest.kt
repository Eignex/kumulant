package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.regression.CovarianceStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class WithSelfLagTest {

    @Test
    fun `first k updates only warm the ring`() {
        val s = CovarianceStat().withSelfLag(k = 3)
        s.update(1.0)
        s.update(2.0)
        s.update(3.0)
        assertEquals(0.0, s.read().totalWeights, DELTA)
    }

    @Test
    fun `alternating pattern gives negative lag-1 autocorrelation`() {
        val s = CovarianceStat().withSelfLag(k = 1)
        repeat(10) { i -> s.update(if (i % 2 == 0) 1.0 else 2.0) }
        // The (current, lag-1) pairs are (2,1),(1,2),(2,1),... giving a negative Pearson.
        val r = s.read()
        assertTrue(r.correlation < -0.5, "correlation=${r.correlation}")
    }

    @Test
    fun `period-2 pattern gives positive lag-2 autocorrelation`() {
        val s = CovarianceStat().withSelfLag(k = 2)
        repeat(20) { i -> s.update(if (i % 2 == 0) 1.0 else 2.0) }
        val r = s.read()
        assertTrue(r.correlation > 0.8, "correlation=${r.correlation}")
    }

    @Test
    fun `k less than one is rejected`() {
        assertFailsWith<IllegalArgumentException> { CovarianceStat().withSelfLag(k = 0) }
    }

    @Test
    fun `reset clears state`() {
        val s = CovarianceStat().withSelfLag(k = 1).apply {
            repeat(20) { update(it.toDouble()) }
        }
        s.reset()
        assertEquals(0.0, s.read().totalWeights, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = CovarianceStat().withSelfLag(k = 1).apply {
            repeat(20) { update(it.toDouble()) }
        }
        val fresh = tpl.create()
        assertEquals(0.0, fresh.read().totalWeights, DELTA)
        assertTrue(tpl.read().totalWeights > 0.0)
    }
}
