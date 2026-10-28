package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.materialize
import com.eignex.kumulant.core.Result
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals

class BanditPolicySpecTest {

    private val json = Json { encodeDefaults = false }

    private fun roundTrip(policy: BanditPolicySpec<*>) {
        @Suppress("UNCHECKED_CAST")
        val wrapped: UnivariateBanditSpec = MultiArmedSpec(
            nbrArms = 3,
            policy = policy as BanditPolicySpec<Result>,
        )
        val encoded = json.encodeToString(UnivariateBanditSpec.serializer(), wrapped)
        val decoded = json.decodeFromString(UnivariateBanditSpec.serializer(), encoded) as MultiArmedSpec<*>
        assertEquals(policy, decoded.policy)
    }

    @Test
    fun `policy configurations round trip and materialize`() {
        val cases: List<BanditPolicySpec<*>> = listOf(
            EpsilonGreedySpec(epsilon = 0.2, priorMean = 0.5, priorWeight = 0.1, priorSquaredDeviations = 0.3),
            EpsilonDecreasingSpec(epsilon = 1.5, decay = 0.7),
            GreedySpec(priorMean = 0.25, priorWeight = 0.05, priorSquaredDeviations = 0.01),
            UniformSelectionSpec(),
            KlUcbSpec(c = 3.0, tolerance = 1e-5, priorAlpha = 2.0, priorBeta = 3.0),
            Ucb1Spec(alpha = 1.5),
            Ucb1NormalSpec(alpha = 0.5, priorMean = 0.1, priorWeight = 0.5),
            Ucb1TunedSpec(alpha = 1.5),
            UcbVSpec(zeta = 1.0, c = 0.5),
            MossSpec(nbrArms = 5, priorMean = 0.0, priorWeight = 0.02),
        )

        for (spec in cases) {
            roundTrip(spec)
            spec.materialize()
        }
    }
}
