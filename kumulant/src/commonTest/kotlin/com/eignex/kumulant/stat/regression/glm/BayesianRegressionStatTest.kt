package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.RegressionStat
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

    // Posteriors captured from the Sherman-Morrison covariance implementation this replaces, so the
    // equivalence is checked against measured behaviour rather than against a re-derivation of it.
    private class CovarianceFormReference(
        val name: String,
        val weights: DoubleArray,
        val bias: Double,
        val covariance: Array<DoubleArray>,
        // The two forms agree to the last few ULPs where the trajectory is multiply-add and sqrt.
        // Logit and Log feed it through `exp`, which each platform rounds its own way, so those
        // cannot be pinned past the point where the platforms themselves stop agreeing.
        val tolerance: Double = 1e-12,
        val fit: () -> BayesianRegressionStat,
    )

    @Test
    fun `read owns vector and matrix storage independently from the stat`() {
        val stat = BayesianRegressionStat(featureSize = 1)
        stat.update(doubleArrayOf(1.0), 1.0)
        val first = stat.read()
        val firstWeight = first.weights[0]
        val firstPrecisionL = first.precisionL[0, 0]

        stat.update(doubleArrayOf(1.0), -1.0)
        BayesianRegressionStat(featureSize = 1).also {
            it.update(doubleArrayOf(1.0), 1.0)
            stat.merge(it.read())
        }
        stat.reset()
        val second = stat.read()

        assertEquals(firstWeight, first.weights[0], 1e-12)
        assertEquals(firstPrecisionL, first.precisionL[0, 0], 1e-12)
        assertEquals(0.0, second.weights[0], 1e-12)
        first.weights.data[0] = 99.0
        first.precisionL.data[0] = 99.0
        assertTrue(second.weights[0] != 99.0)
        assertTrue(second.precisionL[0, 0] != 99.0)
    }

    @Test
    fun `workspace updates and merge match allocating paths`() {
        val allocated = BayesianRegressionStat(featureSize = 2)
        val reused = BayesianRegressionStat(featureSize = 2)
        val workspace = Workspace().apply { reserve(2, 3) }
        repeat(20) { i ->
            val x = F64DenseVector.of(doubleArrayOf(i.toDouble() / 20.0, 1.0))
            allocated.update(x, x[0] + 2.0)
            reused.update(x, x[0] + 2.0, workspace = workspace)
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
    fun `regression stat interface accepts nullable workspace for update and merge`() {
        val workspace = Workspace().apply { reserve(2, 3) }
        val receiver: RegressionStat<PrecisionRegressionResult> = BayesianRegressionStat(featureSize = 2)
        val source: RegressionStat<PrecisionRegressionResult> = BayesianRegressionStat(featureSize = 2)

        receiver.update(doubleArrayOf(1.0, 0.0), 1.0, workspace = null)
        source.update(doubleArrayOf(0.0, 1.0), 2.0, workspace = workspace)
        receiver.merge(source.read(), workspace)

        assertEquals(2.0, receiver.read().totalWeights)
    }

    @Test
    fun `bayesian should recover ground truth and shrink covariance`() {
        val stat = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val truth = doubleArrayOf(0.8, 1.2, -0.5)
        fitLine(stat, truth, intercept = 0.0)
        val r = stat.read()
        val covariance = r.covariance()
        for (i in truth.indices) {
            assertTrue(
                abs(r.weights[i] - truth[i]) < 0.1,
                "weight[$i]=${r.weights[i]} far from truth=${truth[i]}",
            )
        }
        for (i in truth.indices) {
            assertTrue(
                covariance[i, i] < 0.05,
                "Sum[$i,$i]=${covariance[i, i]} did not shrink from prior 1.0",
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
    fun `the precision factor accumulates the exact Gaussian information matrix`() {
        // Under Identity the posterior precision is closed-form: H = H_prior + sum(x xT).
        val stat = BayesianRegressionStat(featureSize = 3, priorVariance = 2.0)
        val rng = Random(3)
        val expected = Array(3) { i -> DoubleArray(3) { j -> if (i == j) 0.5 else 0.0 } }
        repeat(300) {
            val x = DoubleArray(3) { rng.nextDouble() * 2.0 - 1.0 }
            stat.update(x, 1.0)
            for (i in 0 until 3) {
                for (j in 0 until 3) expected[i][j] += x[i] * x[j]
            }
        }

        val l = stat.read().precisionL

        for (i in 0 until 3) {
            for (j in 0 until 3) {
                var h = 0.0
                for (k in 0..minOf(i, j)) h += l[i, k] * l[j, k]
                assertEquals(expected[i][j], h, 1e-9 * maxOf(1.0, abs(expected[i][j])), "H disagrees at ($i, $j)")
            }
        }
    }

    @Test
    fun `an extreme observation weight leaves the posterior finite and positive definite`() {
        for (priorVariance in listOf(1.0, 1e12)) {
            val stat = BayesianRegressionStat(featureSize = 2, priorVariance = priorVariance)

            stat.update(doubleArrayOf(1.0, 1.0), 1.0, 1e18)

            val r = stat.read()
            for (i in 0 until 2) {
                assertTrue(r.precisionL[i, i] > 0.0, "prior $priorVariance left pivot $i at ${r.precisionL[i, i]}")
                assertTrue(r.weights[i].isFinite(), "prior $priorVariance left weight[$i] at ${r.weights[i]}")
            }
        }
    }

    @Test
    fun `the posterior matches the covariance-form reference`() {
        val references = listOf(
            CovarianceFormReference(
                name = "Identity",
                weights = doubleArrayOf(0.7892675161781794, 1.1850458080146096, -0.49356069011182385),
                bias = 0.3073050894951919,
                covariance = arrayOf(
                    doubleArrayOf(0.013290276797415315, 2.1104528854958964E-5, -3.6207626139928776E-4),
                    doubleArrayOf(2.1104528854958964E-5, 0.014407665003666244, 0.0014451418150022467),
                    doubleArrayOf(-3.6207626139928776E-4, 0.0014451418150022467, 0.017524988529503235),
                ),
                fit = {
                    BayesianRegressionStat(featureSize = 3, priorVariance = 1.0).also {
                        fitLine(it, doubleArrayOf(0.8, 1.2, -0.5), intercept = 0.3, n = 200)
                    }
                },
            ),
            CovarianceFormReference(
                name = "Logit",
                weights = doubleArrayOf(1.9835854012108827, -1.0319768995357121),
                bias = 0.04160550210144981,
                covariance = arrayOf(
                    doubleArrayOf(0.07948905510027073, -0.013801112891894552),
                    doubleArrayOf(-0.013801112891894552, 0.07647812815279686),
                ),
                tolerance = 1e-9,
                fit = {
                    BayesianRegressionStat(featureSize = 2, priorVariance = 1.0, link = Link.Logit).also { stat ->
                        val rng = Random(7)
                        repeat(200) {
                            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
                            val p = 1.0 / (1.0 + exp(-(2.0 * x[0] - 1.0 * x[1])))
                            stat.update(x, if (rng.nextDouble() < p) 1.0 else 0.0, 1.0)
                        }
                    }
                },
            ),
            CovarianceFormReference(
                name = "Log",
                weights = doubleArrayOf(0.5912716455018358, 0.13055015954246438),
                bias = 0.21119770261479323,
                covariance = arrayOf(
                    doubleArrayOf(0.07487032541781423, -0.05446353635384874),
                    doubleArrayOf(-0.05446353635384874, 0.07883159690484803),
                ),
                tolerance = 1e-9,
                fit = {
                    BayesianRegressionStat(featureSize = 2, priorVariance = 1.0, link = Link.Log).also { stat ->
                        val rng = Random(2)
                        repeat(200) {
                            val x = doubleArrayOf(rng.nextDouble() * 0.5, rng.nextDouble() * 0.5)
                            stat.update(x, exp(1.0 * x[0] + 0.5 * x[1]), 1.0)
                        }
                    }
                },
            ),
            CovarianceFormReference(
                name = "correlated prior and non-unit weights",
                weights = doubleArrayOf(0.6976030708454642, -0.2986524918934216),
                bias = -1.5549717847883581E-4,
                covariance = arrayOf(
                    doubleArrayOf(0.020610234953868187, -0.004463737865724361),
                    doubleArrayOf(-0.004463737865724361, 0.022249969534956685),
                ),
                fit = {
                    BayesianRegressionStat(
                        featureSize = 2,
                        priorVariance = 1.0,
                        priorMean = F64DenseVector.of(doubleArrayOf(0.25, -0.5)),
                        priorCovariance = F64DenseMatrix.of(
                            arrayOf(doubleArrayOf(2.0, 0.5), doubleArrayOf(0.5, 1.5)),
                        ),
                    ).also { stat ->
                        val rng = Random(11)
                        repeat(50) { i ->
                            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
                            stat.update(x, 0.7 * x[0] - 0.3 * x[1], weight = 0.5 + i * 0.1)
                        }
                    }
                },
            ),
            CovarianceFormReference(
                name = "merge",
                weights = doubleArrayOf(0.5000427206311965, -1.192994020310317, 0.8914216135804326),
                bias = -0.006661823187831848,
                covariance = arrayOf(
                    doubleArrayOf(0.007150695249669388, -4.610474019072746E-4, -3.2990083695119075E-4),
                    doubleArrayOf(-4.610474019072746E-4, 0.0074481219730073035, -3.1298095505277013E-4),
                    doubleArrayOf(-3.2990083695119075E-4, -3.1298095505277013E-4, 0.007005149200908571),
                ),
                fit = {
                    val truth = doubleArrayOf(0.5, -1.2, 0.9)
                    val a = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
                    val b = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
                    fitLine(a, truth, intercept = 0.0, n = 200, seed = 5L)
                    fitLine(b, truth, intercept = 0.0, n = 200, seed = 7L)
                    a.also { it.merge(b.read()) }
                },
            ),
        )

        for (reference in references) {
            val r = reference.fit().read()
            val covariance = r.covariance()
            assertEquals(reference.bias, r.bias, reference.tolerance, "${reference.name} bias")
            for (i in reference.weights.indices) {
                assertEquals(reference.weights[i], r.weights[i], reference.tolerance, "${reference.name} weight[$i]")
                for (j in reference.weights.indices) {
                    assertEquals(
                        reference.covariance[i][j],
                        covariance[i, j],
                        reference.tolerance,
                        "${reference.name} covariance ($i, $j)",
                    )
                }
            }
        }
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
        val mergedCovariance = merged.covariance()

        for (i in truth.indices) {
            assertTrue(
                abs(merged.weights[i] - truth[i]) < 0.1,
                "merged weight[$i]=${merged.weights[i]} far from truth=${truth[i]}",
            )
            // Posterior product should land in the same neighbourhood as replaying
            // all observations into one stat - not pointwise-identical because the
            // Laplace trajectory differs.
            assertTrue(
                abs(merged.weights[i] - refResult.weights[i]) < 0.15,
                "merged weight[$i]=${merged.weights[i]} diverged from replay=${refResult.weights[i]}",
            )
        }
        assertEquals(4000.0, merged.totalWeights, absoluteTolerance = 1e-9)
        // Combined posterior should be at least as tight as each operand.
        for (i in truth.indices) {
            assertTrue(
                mergedCovariance[i, i] < 0.05,
                "merged Sum[$i,$i]=${mergedCovariance[i, i]} did not tighten",
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
