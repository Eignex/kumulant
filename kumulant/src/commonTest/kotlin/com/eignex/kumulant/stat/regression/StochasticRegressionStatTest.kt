package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.schema.Sgd
import kotlin.math.abs
import kotlin.math.exp
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class StochasticRegressionStatTest {

    private fun fitLine(
        stat: RegressionStat<*>,
        slope: DoubleArray,
        intercept: Double,
        n: Int = 4000,
        seed: Long = 42L,
    ) {
        val rng = Random(seed)
        repeat(n) {
            val x = DoubleArray(slope.size) { rng.nextDouble() * 2.0 - 1.0 }
            var y = intercept
            for (i in slope.indices) y += slope[i] * x[i]
            y += rng.nextDouble() * 0.02 - 0.01 // small noise
            stat.update(x, y, 1.0)
        }
    }

    @Test
    fun `sgd should recover ground truth weights`() {
        val stat = StochasticRegressionStat(featureSize = 3, optimizer = Sgd(ConstantRate(0.05)))
        val truth = doubleArrayOf(1.5, -2.0, 0.5)
        fitLine(stat, truth, intercept = 0.3)
        val r = stat.read()
        for (i in truth.indices) {
            assertTrue(
                abs(r.weights[i] - truth[i]) < 0.1,
                "weight[$i]=${r.weights[i]} far from truth=${truth[i]}",
            )
        }
        assertTrue(abs(r.bias - 0.3) < 0.1, "bias=${r.bias} far from 0.3")
    }

    @Test
    fun `merge on SGD blends sample-weighted`() {
        val a = StochasticRegressionStat(featureSize = 2, optimizer = Sgd(ConstantRate(0.05)))
        val b = StochasticRegressionStat(featureSize = 2, optimizer = Sgd(ConstantRate(0.05)))
        val truth = doubleArrayOf(1.0, -1.0)
        fitLine(a, truth, intercept = 0.0, n = 2000, seed = 11L)
        fitLine(b, truth, intercept = 0.0, n = 2000, seed = 22L)
        a.merge(b.read())
        val r = a.read()
        assertEquals(4000.0, r.totalWeights, absoluteTolerance = 1e-9)
        for (i in truth.indices) {
            assertTrue(
                abs(r.weights[i] - truth[i]) < 0.15,
                "merged weight[$i]=${r.weights[i]} far from truth=${truth[i]}",
            )
        }
    }

    @Test
    fun `featureSize mismatch on update throws`() {
        val stat = StochasticRegressionStat(featureSize = 3)
        assertFailsWith<IllegalArgumentException> {
            stat.update(doubleArrayOf(1.0, 2.0), y = 0.0)
        }
    }

    @Test
    fun `SGD with Logit link converges on classification data`() {
        val rng = Random(11)
        val stat = StochasticRegressionStat(
            featureSize = 2,
            optimizer = Sgd(ConstantRate(0.1)),
            link = Link.Logit,
        )
        repeat(3000) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val logit = 1.5 * x[0] - 1.5 * x[1]
            val p = 1.0 / (1.0 + exp(-logit))
            val y = if (rng.nextDouble() < p) 1.0 else 0.0
            stat.update(x, y, 1.0)
        }
        val snap = stat.read()
        assertTrue(snap.weights[0] > 0.0, "w[0] = ${snap.weights[0]} should be positive")
        assertTrue(snap.weights[1] < 0.0, "w[1] = ${snap.weights[1]} should be negative")
        val p1 = snap.predict(DenseVector.of(doubleArrayOf(1.0, -1.0)))
        assertTrue(p1 in 0.0..1.0)
    }

    @Test
    fun `SGD under Concurrency Relaxed converges with no penalty`() {
        val rng = Random(3)
        val stat = StochasticRegressionStat(
            featureSize = 2,
            optimizer = Sgd(ConstantRate(0.05)),
            concurrency = Concurrency.Relaxed,
        )
        repeat(1000) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            stat.update(x, y = 1.0 * x[0] - 2.0 * x[1], weight = 1.0)
        }
        val snap = stat.read()
        // Relaxed mode is still single-threaded in tests; convergence should match Concurrency.None
        assertTrue(abs(snap.weights[0] - 1.0) < 0.3, "w[0] = ${snap.weights[0]}")
        assertTrue(abs(snap.weights[1] - (-2.0)) < 0.3, "w[1] = ${snap.weights[1]}")
    }

    @Test
    fun `SGD with lazy L1 induces sparsity on the irrelevant coordinate`() {
        val rng = Random(4)
        // lambda small enough that the signal coord's gradient beats the threshold,
        // but not so small that irrelevant coords drift far from zero.
        val stat = StochasticRegressionStat(
            featureSize = 3,
            optimizer = Sgd(ConstantRate(0.05)),
            penalty = Penalty.L1(0.01),
        )
        // Only coord 0 matters; coords 1, 2 are irrelevant noise.
        repeat(2000) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble(), rng.nextDouble())
            stat.update(x, y = 1.5 * x[0], weight = 1.0)
        }
        val snap = stat.read()
        // Coord 0 keeps signal (target 1.5; L1 shrinks slightly so > 0.5 is comfortable)
        assertTrue(snap.weights[0] > 0.5, "w[0]=${snap.weights[0]} should be substantially positive")
        // L1 should keep irrelevant coords closer to zero than the signal coord
        assertTrue(
            abs(snap.weights[1]) < snap.weights[0],
            "w[1]=${snap.weights[1]} should be smaller than w[0]=${snap.weights[0]}",
        )
        assertTrue(
            abs(snap.weights[2]) < snap.weights[0],
            "w[2]=${snap.weights[2]} should be smaller than w[0]=${snap.weights[0]}",
        )
    }

    @Test
    fun `SGD with lazy L2 shrinks weights toward zero`() {
        val rng = Random(5)
        // Strong L2 -> weights stay small even with strong signal.
        val statL2 = StochasticRegressionStat(
            featureSize = 2,
            optimizer = Sgd(ConstantRate(0.05)),
            penalty = Penalty.L2(0.5),
        )
        val statNoReg = StochasticRegressionStat(
            featureSize = 2,
            optimizer = Sgd(ConstantRate(0.05)),
        )
        repeat(500) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
            statL2.update(x, y = 5.0 * x[0] + 3.0 * x[1], weight = 1.0)
            statNoReg.update(x, y = 5.0 * x[0] + 3.0 * x[1], weight = 1.0)
        }
        val l2 = statL2.read()
        val no = statNoReg.read()
        // L2 weights should be strictly smaller in magnitude (or near zero) than the unregularised baseline
        assertTrue(abs(l2.weights[0]) <= abs(no.weights[0]) + 0.05)
        assertTrue(abs(l2.weights[1]) <= abs(no.weights[1]) + 0.05)
    }
}
