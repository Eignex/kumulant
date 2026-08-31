package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.Workspace
import com.eignex.kumulant.fitLine
import kotlin.math.abs
import kotlin.math.exp
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
class BayesianRegressionStatTest {

    private class StridedVector(private val backing: DoubleArray) : F64VectorLike {
        override val size: Int get() = backing.size / 2
        override fun get(i: Int): Double = backing[i * 2]
        override fun toDoubleArray(): DoubleArray = DoubleArray(size) { this[it] }
    }

    // The factor is downdated alongside the covariance rather than refactorized, so the two can
    // only be trusted to agree if every downdate lands. L * LT has to stay equal to S. The
    // tolerance is relative because the covariance entries span many orders of magnitude across
    // these tests.
    private fun assertFactorReproducesCovariance(r: CovarianceRegressionResult, relativeTolerance: Double = 1e-9) {
        val n = r.featureSize
        for (i in 0 until n) {
            for (j in 0 until n) {
                var s = 0.0
                for (k in 0..minOf(i, j)) s += r.covarianceL[i, k] * r.covarianceL[j, k]
                val expected = r.covariance[i, j]
                val tol = relativeTolerance * maxOf(1.0, abs(expected))
                assertEquals(expected, s, tol, "L * LT disagrees with the covariance at ($i, $j)")
            }
        }
    }

    @Test
    fun `workspace updates and merge match allocating paths`() {
        val allocated = BayesianRegressionStat(featureSize = 2)
        val reused = BayesianRegressionStat(featureSize = 2)
        val workspace = Workspace().apply { reserve(2, 3) }
        repeat(20) { i ->
            val x = F64DenseVector.of(doubleArrayOf(i.toDouble() / 20.0, 1.0))
            allocated.update(x, x[0] + 2.0)
            reused.update(x, x[0] + 2.0, workspace)
        }
        val other = BayesianRegressionStat(featureSize = 2)
        other.update(doubleArrayOf(0.5, 1.0), 2.5)

        allocated.merge(other.read())
        reused.merge(other.read(), workspace)

        val expected = allocated.read()
        val actual = reused.read()
        for (i in 0 until 2) assertEquals(expected.weights[i], actual.weights[i], 1e-12)
    }

    @Test
    fun `bayesian should recover ground truth and shrink covariance`() {
        val stat = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val truth = doubleArrayOf(0.8, 1.2, -0.5)
        fitLine(stat, truth, intercept = 0.0)
        val r = stat.read()
        for (i in truth.indices) {
            assertTrue(
                abs(r.weights[i] - truth[i]) < 0.1,
                "weight[$i]=${r.weights[i]} far from truth=${truth[i]}",
            )
        }
        for (i in truth.indices) {
            assertTrue(
                r.covariance[i, i] < 0.05,
                "Sum[$i,$i]=${r.covariance[i, i]} did not shrink from prior 1.0",
            )
        }
    }

    @Test
    fun `linear predictor gives generic strided inputs the dense result`() {
        val stat = BayesianRegressionStat(featureSize = 2)
        stat.update(doubleArrayOf(1.0, -2.0), 3.0)
        val snapshot = stat.read()
        val dense = F64DenseVector.of(doubleArrayOf(0.5, -0.25))
        val strided = StridedVector(doubleArrayOf(0.5, 9.0, -0.25, 9.0))

        assertEquals(snapshot.linearPredictor(dense), snapshot.linearPredictor(strided), 1e-12)
    }

    @Test
    fun `the tracked factor keeps reproducing the covariance across updates`() {
        val stat = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        fitLine(stat, doubleArrayOf(0.8, 1.2, -0.5), intercept = 0.3, n = 500)

        assertFactorReproducesCovariance(stat.read())
    }

    @Test
    fun `the factor survives an update that saturates the downdate at the cone boundary`() {
        // A weight this large drives the downdate's norm to exactly 1.0, where it rejects and
        // leaves the factor alone. The repair has to run anyway, or the covariance moves without
        // its factor.
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0)

        stat.update(doubleArrayOf(1.0, 1.0), 1.0, 1e18)

        assertFactorReproducesCovariance(stat.read())
    }

    @Test
    fun `the factor survives a saturating update that the diagonal bump cannot lift off the cone`() {
        // A prior this wide puts the repair's 1e-5 diagonal bump below the covariance's own ULP,
        // so refactorizing reproduces the same factor and the retried downdate saturates at 1.0
        // again. That is the case the shrink step exists for: it scales z down until the downdate
        // lands, and the covariance must be downdated by the same scaled z.
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1e12)

        stat.update(doubleArrayOf(1.0, 1.0), 1.0, 1e18)

        assertFactorReproducesCovariance(stat.read())
    }

    @Test
    fun `merge on Bayesian combines posteriors via density product`() {
        val a = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val b = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val truth = doubleArrayOf(0.5, -1.2, 0.9)
        fitLine(a, truth, intercept = 0.0, n = 2000, seed = 5L)
        fitLine(b, truth, intercept = 0.0, n = 2000, seed = 7L)

        // Reference: a single stat fed the same total data should agree with merge(a, b).
        val ref = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        fitLine(ref, truth, intercept = 0.0, n = 2000, seed = 5L)
        fitLine(ref, truth, intercept = 0.0, n = 2000, seed = 7L)

        a.merge(b.read())
        val merged = a.read()
        val refResult = ref.read()

        for (i in truth.indices) {
            assertTrue(
                abs(merged.weights[i] - truth[i]) < 0.1,
                "merged weight[$i]=${merged.weights[i]} far from truth=${truth[i]}",
            )
            // Posterior product should land in the same neighbourhood as replaying
            // all observations into one stat - not pointwise-identical because SMW
            // accumulates a slightly different trajectory.
            assertTrue(
                abs(merged.weights[i] - refResult.weights[i]) < 0.15,
                "merged weight[$i]=${merged.weights[i]} diverged from replay=${refResult.weights[i]}",
            )
        }
        assertEquals(4000.0, merged.totalWeights, absoluteTolerance = 1e-9)
        // Combined posterior should be at least as tight as each operand.
        for (i in truth.indices) {
            assertTrue(
                merged.covariance[i, i] < 0.05,
                "merged Sum[$i,$i]=${merged.covariance[i, i]} did not tighten",
            )
        }
    }

    @Test
    fun `Identity link is the default and predict returns the linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2)
        assertEquals(Link.Identity, stat.link)
        val snap = stat.read()
        assertEquals(Link.Identity, snap.link)
        val x = F64DenseVector.of(doubleArrayOf(0.5, -0.3))
        assertEquals(snap.linearPredictor(x), snap.predict(x))
    }

    @Test
    fun `Logit link learns a binary classifier from simulated 0_1 data`() {
        val rng = Random(7)
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0, link = Link.Logit)
        repeat(2000) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val logit = 2.0 * x[0] - 1.0 * x[1]
            val p = 1.0 / (1.0 + exp(-logit))
            val y = if (rng.nextDouble() < p) 1.0 else 0.0
            stat.update(x, y, 1.0)
        }
        val snap = stat.read()
        val p1 = snap.predict(F64DenseVector.of(doubleArrayOf(1.0, 0.0)))
        assertTrue(p1 in 0.0..1.0, "predict returned $p1, expected probability")
        assertTrue(p1 > 0.7, "predicted P(positive | (1,0)) = $p1, expected > 0.7")
        val pNeg = snap.predict(F64DenseVector.of(doubleArrayOf(-1.0, 1.0)))
        assertTrue(pNeg < 0.3, "predicted P(positive | (-1,1)) = $pNeg, expected < 0.3")
    }

    @Test
    fun `Log link learns a Poisson rate predictor`() {
        val rng = Random(2)
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0, link = Link.Log)
        repeat(2000) {
            val x = doubleArrayOf(rng.nextDouble() * 0.5, rng.nextDouble() * 0.5)
            val rate = exp(1.0 * x[0] + 0.5 * x[1])
            stat.update(x, rate, 1.0)
        }
        val snap = stat.read()
        val x = F64DenseVector.of(doubleArrayOf(0.5, 0.5))
        val pred = snap.predict(x)
        val expected = exp(0.75)
        assertTrue(abs(pred - expected) / expected < 0.3, "pred=$pred, expected $expected")
    }

    @Test
    fun `Logit predict roundtrip through inverse-link is sigmoid of linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2, link = Link.Logit)
        stat.update(doubleArrayOf(1.0, 0.5), 1.0, 5.0)
        val snap = stat.read()
        val x = F64DenseVector.of(doubleArrayOf(0.7, -0.3))
        val eta = snap.linearPredictor(x)
        val expected = 1.0 / (1.0 + exp(-eta))
        assertEquals(expected, snap.predict(x), absoluteTolerance = 1e-12)
    }

    @Test
    fun `Log predict roundtrip through inverse-link is exp of linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2, link = Link.Log)
        stat.update(doubleArrayOf(0.3, 0.4), 2.0, 1.0)
        val snap = stat.read()
        val x = F64DenseVector.of(doubleArrayOf(0.1, 0.2))
        val eta = snap.linearPredictor(x)
        assertEquals(exp(eta), snap.predict(x), absoluteTolerance = 1e-12)
    }
}
