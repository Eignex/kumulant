package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.feat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The SMW downdate refuses an observation whose scale factor is not a usable number.
 *
 * The guard used to read `if (denom == 0.0)`, which is the one case that cannot occur: `xT S x` is
 * non-negative for a positive-definite `S` and the per-observation precision is non-negative, so `denom`
 * is at least 1. Hitting exactly zero requires an `S` already outside the cone, and in that regime the
 * argument to `sqrt` is *negative*, so the result is NaN and `NaN == 0.0` is false. The guard's only
 * reachable input was the one it failed to catch.
 *
 * The cheaper path to the same failure needs no instability at all: `Link.Log.curvature` is `exp(eta)`,
 * which overflows to `+Infinity` for a large linear predictor. Then the numerator and denominator are
 * both infinite and their ratio is NaN.
 *
 * Why it matters more here than elsewhere: the NaN does not stop at the rejected observation. It reaches
 * `ger`, which writes it into the covariance, and every later prediction reads through that covariance.
 * The model is dead from then on with nothing in the result saying so. A non-finite value is allowed to
 * poison a stat under [com.eignex.kumulant.core.Stat]'s contract, but here it is the *stat's own*
 * arithmetic manufacturing the NaN out of finite inputs, which is a defect rather than propagation.
 */
class BayesianDenomGuardTest {

    @Test
    fun `an overflowing Poisson curvature does not poison the covariance`() {
        // Log link, and a linear predictor driven far enough up that exp(eta) overflows. Every input
        // here is finite: the feature is 1.0, the target is a plausible count, the weight is 1.0.
        val stat = BayesianRegressionStat(featureSize = 1, link = Link.Log, priorVariance = 1e6)

        repeat(200) { stat.update(feat(1.0), 1e5, 1.0) }

        val r = stat.read()
        assertTrue(r.weights[0].isFinite(), "the coefficient went non-finite: ${r.weights[0]}")
        assertTrue(r.bias.isFinite(), "the bias went non-finite: ${r.bias}")
        assertTrue(r.covariance[0, 0].isFinite(), "the covariance went non-finite: ${r.covariance[0, 0]}")
    }

    @Test
    fun `the fit survives the first observation instead of collapsing on the second`() {
        // Measured behaviour, before and after. On the unfixed code the first observation lands and the
        // *second* turns every field to NaN, permanently:
        //
        //     i=1  w=99998.90001187997  cov=0.9999990001088008
        //     i=2  w=NaN                cov=NaN
        //
        // With the guard widened, the fit from the first observation stays put and each later one is
        // refused, because `exp(eta)` for that linear predictor is infinite and there is no finite
        // update to apply. Frozen at a real fit beats NaN: the caller still gets the numbers the data
        // supported, and `totalWeights` still says how many observations were absorbed.
        val stat = BayesianRegressionStat(featureSize = 1, link = Link.Log, priorVariance = 1e6)

        stat.update(feat(1.0), 1e5, 1.0)
        val afterFirst = stat.read().weights[0]
        repeat(50) { stat.update(feat(1.0), 1e5, 1.0) }

        val r = stat.read()
        assertTrue(!r.weights[0].isNaN(), "the coefficient collapsed to NaN")
        assertTrue(!r.covariance[0, 0].isNaN(), "the covariance collapsed to NaN")
        assertEquals(afterFirst, r.weights[0], "the refused observations should have left the fit alone")

        // `predict` is `exp(eta)` under the Log link, so it overflows to Infinity for a linear predictor
        // this large. That is the correct answer for the fitted model, not corruption - the distinction
        // the assertions above are drawn around. A NaN would mean the arithmetic broke.
        assertTrue(!r.predict(feat(1.0)).isNaN(), "prediction is NaN, so the covariance is still poisoned")
    }

    @Test
    fun `an ordinary Poisson stream still trains`() {
        // Guards the two above: a stat that refused every observation would satisfy them. The Log link
        // has to keep working on the counts it is actually for.
        val stat = BayesianRegressionStat(featureSize = 1, link = Link.Log)

        val before = stat.read().weights[0]
        for (i in 1..200) stat.update(feat(if (i % 2 == 0) 1.0 else -1.0), (i % 5).toDouble(), 1.0)

        val r = stat.read()
        assertTrue(r.weights[0] != before, "the Log-link model ignored a perfectly ordinary stream")
        assertTrue(r.weights[0].isFinite(), "the coefficient went non-finite: ${r.weights[0]}")
    }
}
