package com.eignex.kumulant.bandit

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.stat.regression.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.LinUcb
import com.eignex.kumulant.stat.regression.MultivariateGaussian
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RegressionContextualBanditTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    @Test
    fun `constructor rejects non-positive nbrArms`() {
        assertFailsWith<IllegalArgumentException> {
            RegressionContextualBandit(
                nbrArms = 0,
                template = BayesianRegressionStat(featureSize = 2),
                posterior = MultivariateGaussian,
            )
        }
    }

    @Test
    fun `choose converges to the arm with the best linear payoff`() {
        // Three arms with different true weight vectors; the bandit should learn
        // which one pays best on the average context.
        val trueWeights = listOf(
            doubleArrayOf(1.0, 0.0),
            doubleArrayOf(0.0, 1.0),
            doubleArrayOf(0.5, 0.5),
        )
        val rng = Random(1)
        val bandit = RegressionContextualBandit(
            nbrArms = 3,
            template = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0),
            posterior = MultivariateGaussian,
            random = rng,
        )

        repeat(3000) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val xv = DenseVector.of(x)
            val arm = bandit.choose(xv)
            val reward = trueWeights[arm][0] * x[0] + trueWeights[arm][1] * x[1] +
                rng.nextDouble() * 0.1 - 0.05
            bandit.update(arm, xv, reward)
        }

        // At feature (1, 0), arm 0 is best; at (0, 1), arm 1 is best.
        val picksAt10 = IntArray(3)
        val picksAt01 = IntArray(3)
        repeat(200) {
            picksAt10[bandit.choose(feat(1.0, 0.0))]++
            picksAt01[bandit.choose(feat(0.0, 1.0))]++
        }
        assertTrue(picksAt10[0] > picksAt10[1], "arm 0 should dominate at (1,0): $picksAt10")
        assertTrue(picksAt10[0] > picksAt10[2], "arm 0 should dominate at (1,0): $picksAt10")
        assertTrue(picksAt01[1] > picksAt01[0], "arm 1 should dominate at (0,1): $picksAt01")
    }

    @Test
    fun `evaluate is reproducible under a fixed seed`() {
        // Same RNG seed, same snapshot -> same score. Catches accidental shared mutable
        // state on the posterior.
        val rng = Random(0)
        val bandit = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = LinUcb,
            random = rng,
        )
        bandit.update(0, feat(1.0, 0.0), 1.0)
        bandit.update(1, feat(0.0, 1.0), 2.0)
        val s0 = bandit.evaluate(0, feat(1.0, 0.0))
        val s0Again = bandit.evaluate(0, feat(1.0, 0.0))
        assertEquals(s0, s0Again, "LinUcb is deterministic given a snapshot")
        assertTrue(s0 > 0.0)
    }

    @Test
    fun `armStat exposes the live per-arm regressor`() {
        val bandit = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(0),
        )
        bandit.update(0, feat(1.0, 0.0), 5.0)
        bandit.update(0, feat(1.0, 0.0), 5.0)
        // Reading via armStat must match armResult and snapshot.
        assertEquals(bandit.armResult(0), bandit.armStat(0).read(0L))
        assertEquals(bandit.snapshot()[0], bandit.armResult(0))
    }

    @Test
    fun `merge fans per-arm snapshots through to sub-stats`() {
        val ba = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(1),
        )
        val bb = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(2),
        )
        repeat(200) {
            ba.update(0, feat(1.0, 0.0), 1.0)
            ba.update(1, feat(0.0, 1.0), -1.0)
        }
        repeat(200) {
            bb.update(0, feat(1.0, 0.0), 1.0)
            bb.update(1, feat(0.0, 1.0), -1.0)
        }
        val merged = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(3),
        )
        merged.merge(ba.snapshot())
        merged.merge(bb.snapshot())
        // Merged bandit should have higher total weight per arm than either replica.
        val mergedW = merged.armResult(0).totalWeights
        val singleW = ba.armResult(0).totalWeights
        assertTrue(mergedW >= singleW, "merged $mergedW should >= single $singleW")
    }

    @Test
    fun `merge rejects size mismatch`() {
        val bandit = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
        )
        val wrongSize = RegressionContextualBandit(
            nbrArms = 3,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
        ).snapshot()
        assertFailsWith<IllegalArgumentException> { bandit.merge(wrongSize) }
    }

    @Test
    fun `reset restores prior baseline`() {
        val bandit = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0),
            posterior = MultivariateGaussian,
            random = Random(0),
        )
        repeat(200) { bandit.update(0, feat(1.0, 0.0), 5.0) }
        val before = bandit.armResult(0).totalWeights
        bandit.reset()
        val after = bandit.armResult(0).totalWeights
        assertTrue(before > 0.0)
        assertEquals(0.0, after, "reset zeros accumulated weight")
    }

    @Test
    fun `create returns a fresh bandit with the same configuration`() {
        val original = RegressionContextualBandit(
            nbrArms = 4,
            template = BayesianRegressionStat(featureSize = 3),
            posterior = MultivariateGaussian,
            exploration = 0.5,
            random = Random(7),
        )
        repeat(50) { original.update(0, feat(1.0, 0.0, 0.0), 1.0) }
        val fresh = original.create()
        assertEquals(4, fresh.nbrArms)
        assertEquals(0.0, fresh.armResult(0).totalWeights, "fresh instance has no data")
        // Original is untouched.
        assertTrue(original.armResult(0).totalWeights > 0.0)
    }

    @Test
    fun `pooled bandit globalSnapshot is null when pooling is disabled`() {
        val bandit = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
        )
        assertNull(bandit.globalSnapshot())
        assertNull(bandit.globalStat())
    }

    @Test
    fun `pooled bandit globalSnapshot is non-null when pooling is enabled`() {
        val bandit = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            globalTemplate = BayesianRegressionStat(featureSize = 2),
            random = Random(0),
        )
        assertNotNull(bandit.globalSnapshot())
        assertNotNull(bandit.globalStat())
        repeat(50) {
            bandit.update(0, feat(1.0, 0.0), 1.0)
        }
        // Global has absorbed updates regardless of arm
        assertTrue(bandit.globalSnapshot()!!.totalWeights > 0.0)
    }

    @Test
    fun `pooled bandit feeds residuals to per-arm regressors`() {
        // When pooling is on and the global has fitted some signal, the per-arm regressors
        // should track delta-from-global, not raw reward.
        val rng = Random(1)
        val pooled = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            globalTemplate = BayesianRegressionStat(featureSize = 2),
            random = rng,
        )
        // Both arms see the same reward function -> per-arm residuals should stay near zero,
        // while the global picks up the signal.
        repeat(500) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
            val xv = DenseVector.of(x)
            val reward = 2.0 * x[0] + 0.5 * x[1]
            pooled.update(0, xv, reward)
            pooled.update(1, xv, reward)
        }
        val globalSnap = pooled.globalSnapshot()!!
        // Global weights should approximate (2.0, 0.5)
        assertTrue(abs(globalSnap.weights[0] - 2.0) < 0.5, "global w[0]=${globalSnap.weights[0]}")
        assertTrue(abs(globalSnap.weights[1] - 0.5) < 0.5, "global w[1]=${globalSnap.weights[1]}")
        // Per-arm weights should be small (residual ~ 0)
        val arm0 = pooled.armResult(0)
        assertTrue(abs(arm0.weights[0]) < 0.5, "arm0 residual w[0]=${arm0.weights[0]} should be near 0")
    }

    @Test
    fun `pooled bandit mergeGlobal accumulates cross-replica global`() {
        val ba = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            globalTemplate = BayesianRegressionStat(featureSize = 2),
            random = Random(1),
        )
        val bb = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            globalTemplate = BayesianRegressionStat(featureSize = 2),
            random = Random(2),
        )
        repeat(50) { ba.update(0, feat(1.0, 0.0), 1.0) }
        repeat(50) { bb.update(0, feat(1.0, 0.0), 1.0) }
        val mergeable = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            globalTemplate = BayesianRegressionStat(featureSize = 2),
            random = Random(3),
        )
        val gA = ba.globalSnapshot()!!
        val gB = bb.globalSnapshot()!!
        mergeable.mergeGlobal(gA)
        mergeable.mergeGlobal(gB)
        val mergedTotal = mergeable.globalSnapshot()!!.totalWeights
        assertTrue(mergedTotal > 0.0, "global was updated by mergeGlobal")
    }

    @Test
    fun `mergeGlobal is a no-op when pooling is disabled`() {
        val bandit = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
        )
        val otherGlobal = BayesianRegressionStat(featureSize = 2).also {
            it.update(doubleArrayOf(1.0, 0.0), 1.0)
        }.read()
        // Should not throw
        bandit.mergeGlobal(otherGlobal)
        assertNull(bandit.globalSnapshot())
    }

    @Test
    fun `reset clears global when pooling is enabled`() {
        val bandit = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            globalTemplate = BayesianRegressionStat(featureSize = 2),
            random = Random(0),
        )
        repeat(100) { bandit.update(0, feat(1.0, 0.0), 1.0) }
        assertTrue(bandit.globalSnapshot()!!.totalWeights > 0.0)
        bandit.reset()
        assertEquals(0.0, bandit.globalSnapshot()!!.totalWeights)
    }
}
