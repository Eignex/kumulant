package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.feat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// All inputs are finite; the Log link itself overflows while computing curvature.
class BayesianCurvatureGuardTest {

    @Test
    fun `an overflowing Poisson curvature does not poison the precision factor`() {
        val stat = BayesianRegressionStat(featureSize = 1, link = Link.Log, priorVariance = 1e6)

        repeat(200) { stat.update(feat(1.0), 1e5, 1.0) }

        val r = stat.read()
        assertTrue(r.weights[0].isFinite(), "the coefficient went non-finite: ${r.weights[0]}")
        assertTrue(r.bias.isFinite(), "the bias went non-finite: ${r.bias}")
        assertTrue(r.precisionL[0, 0].isFinite(), "the precision factor went non-finite: ${r.precisionL[0, 0]}")
    }

    @Test
    fun `the fit survives the first observation instead of collapsing on the second`() {
        val stat = BayesianRegressionStat(featureSize = 1, link = Link.Log, priorVariance = 1e6)

        stat.update(feat(1.0), 1e5, 1.0)
        val afterFirst = stat.read().weights[0]
        repeat(50) { stat.update(feat(1.0), 1e5, 1.0) }

        val r = stat.read()
        assertTrue(!r.weights[0].isNaN(), "the coefficient collapsed to NaN")
        assertTrue(!r.precisionL[0, 0].isNaN(), "the precision factor collapsed to NaN")
        assertEquals(afterFirst, r.weights[0], "the refused observations should have left the fit alone")

        assertTrue(!r.predict(feat(1.0)).isNaN(), "prediction is NaN, so the posterior is still poisoned")
    }

    @Test
    fun `an ordinary Poisson stream still trains`() {
        val stat = BayesianRegressionStat(featureSize = 1, link = Link.Log)

        val before = stat.read().weights[0]
        for (i in 1..200) stat.update(feat(if (i % 2 == 0) 1.0 else -1.0), (i % 5).toDouble(), 1.0)

        val r = stat.read()
        assertTrue(r.weights[0] != before, "the Log-link model ignored a perfectly ordinary stream")
        assertTrue(r.weights[0].isFinite(), "the coefficient went non-finite: ${r.weights[0]}")
    }
}
