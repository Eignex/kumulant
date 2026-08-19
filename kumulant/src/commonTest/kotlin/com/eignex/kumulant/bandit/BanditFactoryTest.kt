package com.eignex.kumulant.bandit

import com.eignex.kumulant.bandit.contextual.ContextualBanditSpec
import com.eignex.kumulant.bandit.contextual.KnnContextualSpec
import com.eignex.kumulant.bandit.contextual.KnnDistance
import com.eignex.kumulant.bandit.contextual.LinearRegressionSpec
import com.eignex.kumulant.bandit.contextual.RegressionContextualSpec
import com.eignex.kumulant.bandit.univariate.BernoulliArm
import com.eignex.kumulant.bandit.univariate.BetaPosterior
import com.eignex.kumulant.bandit.univariate.BoltzmannSpec
import com.eignex.kumulant.bandit.univariate.Exp3Bandit
import com.eignex.kumulant.bandit.univariate.Exp3Spec
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.MossSpec
import com.eignex.kumulant.bandit.univariate.MultiArmedSpec
import com.eignex.kumulant.bandit.univariate.NormalArm
import com.eignex.kumulant.bandit.univariate.NormalGammaPosterior
import com.eignex.kumulant.bandit.univariate.RouletteWheelSpec
import com.eignex.kumulant.bandit.univariate.ThompsonSamplingSpec
import com.eignex.kumulant.bandit.univariate.TopTwoThompsonSpec
import com.eignex.kumulant.bandit.univariate.Ucb1Spec
import com.eignex.kumulant.bandit.univariate.UnivariateBanditSpec
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import kotlinx.serialization.json.Json
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BanditFactoryTest {

    private val json = Json { encodeDefaults = false }

    @Test
    fun `MultiArmed spec materialises to a working MultiArmedBandit`() {
        val spec = MultiArmedSpec(
            nbrArms = 3,
            policy = ThompsonSamplingSpec(BernoulliArm(), BetaPosterior),
        )
        val b = (spec as UnivariateBanditSpec).materialize(Random(0))
        assertTrue(b is MultiArmedBandit<*>)
        assertEquals(3, b.nbrArms)
    }

    @Test
    fun `RouletteWheel spec materialises`() {
        val b = RouletteWheelSpec(nbrArms = 4, segmentLength = 5).materialize(Random(0))
        assertEquals(4, b.nbrArms)
    }

    @Test
    fun `Boltzmann spec materialises`() {
        val b = BoltzmannSpec(nbrArms = 3).materialize(Random(0))
        assertEquals(3, b.nbrArms)
    }

    @Test
    fun `Exp3 spec materialises with default eta and gamma`() {
        val b = Exp3Spec(nbrArms = 3).materialize(Random(0))
        assertEquals(3, b.nbrArms)
    }

    @Test
    fun `a spec-built Exp3 resolves the same gamma as the constructor`() {
        for (arms in 2..12) {
            val fromSpec = Exp3Spec(nbrArms = arms).materialize(Random(0))
            val direct = Exp3Bandit(nbrArms = arms, random = Random(0))
            assertEquals(direct.gamma, fromSpec.gamma, "gamma disagrees at K=$arms")
            assertTrue(fromSpec.gamma < 1.0, "gamma saturated at K=$arms, so the play distribution is uniform")
        }
    }

    @Test
    fun `a spec-built Exp3 lets its weights steer the play distribution`() {
        val b = Exp3Spec(nbrArms = 3).materialize(Random(0))
        repeat(50) { b.update(armIndex = 0, value = 1.0) }

        val p = b.playDistribution()
        assertTrue(p[0] > p[1], "the rewarded arm should be played more often: ${p.toList()}")
    }

    @Test
    fun `TopTwoThompson spec materialises`() {
        val b = TopTwoThompsonSpec(
            nbrArms = 3,
            policy = ThompsonSamplingSpec(NormalArm(), NormalGammaPosterior),
        ).materialize(Random(0))
        assertEquals(3, b.nbrArms)
    }

    @Test
    fun `RegressionContextual spec materialises with Bayesian backbone`() {
        val spec = RegressionContextualSpec(
            nbrArms = 4,
            regression = LinearRegressionSpec.Bayesian(featureSize = 2, priorVariance = 1.5),
            posterior = MultivariateGaussian,
        )
        val b = spec.materialize(Random(0))
        assertEquals(4, b.nbrArms)
    }

    @Test
    fun `KnnContextual spec materialises with the default distance`() {
        val b = KnnContextualSpec(nbrArms = 3).materialize(Random(0))
        assertEquals(3, b.nbrArms)
    }

    @Test
    fun `the knn distance survives a round trip and is not a bare string`() {
        // A sealed metric means an unknown one is a compile error, and the wire format admits only
        // metrics that exist rather than any string at all.
        val spec: ContextualBanditSpec = KnnContextualSpec(nbrArms = 3, distance = KnnDistance.SquaredL2)

        // `json` here omits defaults, and SquaredL2 is the default, so a second encoder is needed to see
        // the metric on the wire at all. That it round-trips under the terse encoder is the other half.
        val verbose = Json { encodeDefaults = true }
        val encoded = verbose.encodeToString(ContextualBanditSpec.serializer(), spec)
        assertTrue("SquaredL2" in encoded, "the metric did not reach the wire: $encoded")

        assertEquals(spec, verbose.decodeFromString(ContextualBanditSpec.serializer(), encoded))
        assertEquals(
            spec,
            json.decodeFromString(
                ContextualBanditSpec.serializer(),
                json.encodeToString(ContextualBanditSpec.serializer(), spec),
            ),
        )
    }

    @Test
    fun `univariate spec round-trips through JSON`() {
        val spec: UnivariateBanditSpec = MultiArmedSpec(
            nbrArms = 4,
            policy = Ucb1Spec(alpha = 1.5),
        )
        val encoded = json.encodeToString(UnivariateBanditSpec.serializer(), spec)
        val decoded = json.decodeFromString(UnivariateBanditSpec.serializer(), encoded)
        assertEquals(spec, decoded)
    }

    @Test
    fun `MultiArmedSpec rejects a Moss policy with a different arm count`() {
        assertFailsWith<IllegalArgumentException> {
            MultiArmedSpec(nbrArms = 3, policy = MossSpec(nbrArms = 5)).materialize()
        }
    }
}
