package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.stat.regression.glm.ConstantRate
import kotlin.math.abs
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OptimizerDirectTest {

    private fun approx(expected: Double, actual: Double, tol: Double = 1e-12) {
        assertTrue(abs(expected - actual) <= tol, "expected ~$expected, got $actual")
    }

    @Test
    fun `Sgd delta uses cached learning rate and respects observation weight`() {
        val opt = SgdOptimizer(featureSize = 2, learningRate = ConstantRate(0.1))
        opt.advance()
        assertEquals(1L, opt.step)
        approx(-0.1 * 2.0 * 3.0, opt.computeDelta(0, gradient = 3.0, observationWeight = 2.0))
        opt.advance()
        assertEquals(2L, opt.step)
    }

    @Test
    fun `Sgd reset zeroes step counter`() {
        val opt = SgdOptimizer(featureSize = 1, learningRate = ConstantRate(0.5))
        opt.advance()
        opt.advance()
        opt.reset()
        assertEquals(0L, opt.step)
    }

    @Test
    fun `Adam bias-corrects first and second moments on step 1`() {
        val b1 = 0.9
        val b2 = 0.999
        val eps = 1e-8
        val opt = AdamOptimizer(
            featureSize = 1,
            learningRate = ConstantRate(0.1),
            beta1 = b1,
            beta2 = b2,
            epsilon = eps,
        )
        opt.advance()
        val g = 2.0
        val mNext = (1.0 - b1) * g
        val vNext = (1.0 - b2) * g * g
        val mHat = mNext / (1.0 - b1)
        val vHat = vNext / (1.0 - b2)
        approx(-0.1 * mHat / (sqrt(vHat) + eps), opt.computeDelta(0, gradient = g, observationWeight = 1.0))
    }

    @Test
    fun `Adam reset clears moments so a fresh advance behaves like step 1`() {
        val opt = AdamOptimizer(featureSize = 1, learningRate = ConstantRate(0.1))
        opt.advance()
        opt.computeDelta(0, gradient = 5.0, observationWeight = 1.0)
        opt.advance()
        opt.computeDelta(0, gradient = -1.0, observationWeight = 1.0)
        opt.reset()
        opt.advance()
        val d1 = opt.computeDelta(0, gradient = 2.0, observationWeight = 1.0)
        val fresh = AdamOptimizer(featureSize = 1, learningRate = ConstantRate(0.1))
        fresh.advance()
        val d2 = fresh.computeDelta(0, gradient = 2.0, observationWeight = 1.0)
        approx(d2, d1)
    }

    @Test
    fun `Adam validates beta and epsilon`() {
        assertFailsWith<IllegalArgumentException> {
            AdamOptimizer(featureSize = 1, learningRate = ConstantRate(0.1), beta1 = -0.1)
        }
        assertFailsWith<IllegalArgumentException> {
            AdamOptimizer(featureSize = 1, learningRate = ConstantRate(0.1), beta2 = 1.5)
        }
        assertFailsWith<IllegalArgumentException> {
            AdamOptimizer(featureSize = 1, learningRate = ConstantRate(0.1), epsilon = 0.0)
        }
    }

    @Test
    fun `Adam advance increments step counter`() {
        val opt = AdamOptimizer(featureSize = 1, learningRate = ConstantRate(0.1))
        repeat(5) { opt.advance() }
        val d5 = opt.computeDelta(0, gradient = 1.0, observationWeight = 1.0)
        val fresh = AdamOptimizer(featureSize = 1, learningRate = ConstantRate(0.1))
        fresh.advance()
        val d1 = fresh.computeDelta(0, gradient = 1.0, observationWeight = 1.0)
        assertTrue(abs(d5 - d1) > 1e-6, "expected step-dependent delta to differ; d1=$d1 d5=$d5")
    }
}
