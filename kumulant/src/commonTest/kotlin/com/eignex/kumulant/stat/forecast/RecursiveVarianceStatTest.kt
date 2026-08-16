package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RecursiveVarianceStatTest {

    @Test
    fun `first update applies omega plus alpha times value squared`() {
        val s = RecursiveVarianceStat(omega = 0.5, alpha = 0.1, beta = 0.9)
        s.update(2.0)
        // omega + alpha * 4 + beta * 0 = 0.5 + 0.4 = 0.9
        assertEquals(0.9, s.read().variance, DELTA)
    }

    @Test
    fun `recurrence applies beta times previous`() {
        val s = RecursiveVarianceStat(omega = 0.0, alpha = 0.5, beta = 0.5)
        s.update(2.0) // 0 + 0.5*4 + 0.5*0 = 2.0
        s.update(0.0) // 0 + 0 + 0.5*2 = 1.0
        assertEquals(1.0, s.read().variance, DELTA)
    }

    @Test
    fun `zero coefficients leave variance at zero`() {
        val s = RecursiveVarianceStat(omega = 0.0, alpha = 0.0, beta = 0.0)
        for (v in listOf(1.0, 2.0, 3.0)) s.update(v)
        assertEquals(0.0, s.read().variance, DELTA)
    }

    @Test
    fun `result carries the configured coefficients`() {
        val r = RecursiveVarianceStat(omega = 0.1, alpha = 0.2, beta = 0.3).read()
        assertEquals(0.1, r.omega, DELTA)
        assertEquals(0.2, r.alpha, DELTA)
        assertEquals(0.3, r.beta, DELTA)
    }

    @Test
    fun `negative coefficients are rejected`() {
        assertFailsWith<IllegalArgumentException> { RecursiveVarianceStat(omega = -0.1, alpha = 0.0, beta = 0.0) }
        assertFailsWith<IllegalArgumentException> { RecursiveVarianceStat(omega = 0.0, alpha = -0.1, beta = 0.0) }
        assertFailsWith<IllegalArgumentException> { RecursiveVarianceStat(omega = 0.0, alpha = 0.0, beta = -0.1) }
    }

    @Test
    fun `reset clears variance`() {
        val s = RecursiveVarianceStat(omega = 0.1, alpha = 0.1, beta = 0.5).apply {
            update(5.0)
            update(5.0)
        }
        s.reset()
        assertEquals(0.0, s.read().variance, DELTA)
    }
}
