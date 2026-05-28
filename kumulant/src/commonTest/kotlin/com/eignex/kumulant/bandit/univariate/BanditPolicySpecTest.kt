package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.materialize
import com.eignex.kumulant.core.Result
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class BanditPolicySpecTest {

    private val json = Json { encodeDefaults = false }

    private fun roundTrip(policy: BanditPolicySpec<*>) {
        // Wrap in MultiArmedSpec so the BanditPolicySpec field is serialized polymorphically
        // through the sealed UnivariateBanditSpec hierarchy.
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
    fun `EpsilonGreedySpec round-trips and materialises`() {
        val spec = EpsilonGreedySpec(epsilon = 0.2, priorMean = 0.5, priorWeight = 0.1, priorSquaredDeviations = 0.3)
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }

    @Test
    fun `EpsilonDecreasingSpec round-trips and materialises`() {
        val spec = EpsilonDecreasingSpec(epsilon = 1.5, decay = 0.7)
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }

    @Test
    fun `GreedySpec round-trips and materialises`() {
        val spec = GreedySpec(priorMean = 0.25, priorWeight = 0.05, priorSquaredDeviations = 0.01)
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }

    @Test
    fun `UniformSelectionSpec round-trips and materialises`() {
        val spec = UniformSelectionSpec()
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }

    @Test
    fun `KlUcbSpec round-trips and materialises`() {
        val spec = KlUcbSpec(c = 3.0, tolerance = 1e-5, priorAlpha = 2.0, priorBeta = 3.0)
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }

    @Test
    fun `Ucb1NormalSpec round-trips and materialises`() {
        val spec = Ucb1NormalSpec(alpha = 0.5, priorMean = 0.1, priorWeight = 0.5)
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }

    @Test
    fun `Ucb1TunedSpec round-trips and materialises`() {
        val spec = Ucb1TunedSpec(alpha = 1.5)
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }

    @Test
    fun `UcbVSpec round-trips and materialises`() {
        val spec = UcbVSpec(zeta = 1.0, c = 0.5)
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }

    @Test
    fun `MossSpec round-trips and materialises`() {
        val spec = MossSpec(nbrArms = 5, priorMean = 0.0, priorWeight = 0.02)
        roundTrip(spec)
        assertNotNull(spec.materialize())
    }
}
