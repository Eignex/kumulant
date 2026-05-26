package com.eignex.kumulant.bandit

import com.eignex.kumulant.bandit.contextual.KnnContextualBandit
import com.eignex.kumulant.bandit.contextual.KnnContextualSpec
import com.eignex.kumulant.bandit.contextual.LinearRegressionSpec
import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.contextual.RegressionContextualSpec
import com.eignex.kumulant.bandit.univariate.BernoulliArm
import com.eignex.kumulant.bandit.univariate.BetaPosterior
import com.eignex.kumulant.bandit.univariate.BoltzmannBandit
import com.eignex.kumulant.bandit.univariate.BoltzmannSpec
import com.eignex.kumulant.bandit.univariate.Exp3Bandit
import com.eignex.kumulant.bandit.univariate.Exp3Spec
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.MultiArmedSpec
import com.eignex.kumulant.bandit.univariate.NormalArm
import com.eignex.kumulant.bandit.univariate.NormalGammaPosterior
import com.eignex.kumulant.bandit.univariate.RouletteWheelBandit
import com.eignex.kumulant.bandit.univariate.RouletteWheelSpec
import com.eignex.kumulant.bandit.univariate.ThompsonSamplingSpec
import com.eignex.kumulant.bandit.univariate.TopTwoThompsonBandit
import com.eignex.kumulant.bandit.univariate.TopTwoThompsonSpec
import com.eignex.kumulant.bandit.univariate.Ucb1Spec
import com.eignex.kumulant.bandit.univariate.UnivariateBanditSpec
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import kotlinx.serialization.json.Json
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
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
        assertTrue(b is RouletteWheelBandit)
    }

    @Test
    fun `Boltzmann spec materialises`() {
        val b = BoltzmannSpec(nbrArms = 3).materialize(Random(0))
        assertEquals(3, b.nbrArms)
        assertTrue(b is BoltzmannBandit)
    }

    @Test
    fun `Exp3 spec materialises with default eta and gamma`() {
        val b = Exp3Spec(nbrArms = 3).materialize(Random(0))
        assertEquals(3, b.nbrArms)
        assertTrue(b is Exp3Bandit)
    }

    @Test
    fun `TopTwoThompson spec materialises`() {
        val b = TopTwoThompsonSpec(
            nbrArms = 3,
            policy = ThompsonSamplingSpec(NormalArm(), NormalGammaPosterior),
        ).materialize(Random(0))
        assertEquals(3, b.nbrArms)
        assertTrue(b is TopTwoThompsonBandit<*>)
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
        assertTrue(b is RegressionContextualBandit<*>)
    }

    @Test
    fun `KnnContextual spec materialises with the default distance`() {
        val b = KnnContextualSpec(nbrArms = 3).materialize(Random(0))
        assertEquals(3, b.nbrArms)
        assertTrue(b is KnnContextualBandit)
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
}
