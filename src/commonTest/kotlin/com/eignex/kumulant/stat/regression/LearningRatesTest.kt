package com.eignex.kumulant.stat.regression

import kotlin.math.abs
import kotlin.math.exp
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LearningRatesTest {

    @Test
    fun `ConstantRate is independent of step`() {
        val eta = ConstantRate(0.07)
        assertEquals(0.07, eta.eval(0.0))
        assertEquals(0.07, eta.eval(123.0))
    }

    @Test
    fun `StepDecay halves at the configured horizon`() {
        val eta = 0.02; val k = 0.01
        val sched = StepDecay(eta, k)
        assertEquals(eta, sched.eval(0.0), absoluteTolerance = 1e-12)
        assertEquals(eta / 2.0, sched.eval(1.0 / k), absoluteTolerance = 1e-12)
        assertTrue(sched.eval(1000.0) < sched.eval(10.0))
    }

    @Test
    fun `ExponentialDecay matches eta times exp minus k step`() {
        val eta = 0.1; val k = 0.05
        val sched = ExponentialDecay(eta, k)
        assertEquals(eta, sched.eval(0.0), absoluteTolerance = 1e-12)
        for (step in listOf(1.0, 5.0, 50.0)) {
            val expected = eta * exp(-k * step)
            assertTrue(abs(sched.eval(step) - expected) < 1e-12)
        }
    }
}
