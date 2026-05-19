package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import kotlin.random.Random

/**
 * Root of every bandit kumulant ships. Carries the bare minimum every flavour
 * needs: arm count, a randomness source, and a way to wipe state back to its
 * prior-seeded baseline. The choose/update surface lives on the sibling
 * [UnivariateBandit] (indexless arms) and [ContextualBandit] (per-round
 * context vector) interfaces.
 *
 * State management — snapshot/merge/recreation — is orthogonal to action; see
 * [Snapshotable] and the [PerArmBandit] convenience for the common case.
 * Per-arm scoring (e.g. for inspection) is opted into via [Scorable] /
 * [ContextualScorable] — bandits that select arms via joint sampling
 * (Boltzmann, Top-Two Thompson) don't expose a per-arm score.
 */
interface Bandit {
    /** Number of arms in the population. Fixed at construction. */
    val nbrArms: Int

    /** Single source of randomness for [choose] and any policy-internal sampling. */
    val random: Random

    /** Clear all state back to the prior-seeded baseline. */
    fun reset()
}

/**
 * Online optimizer over a fixed set of unindexed arms. Each round the user
 * calls [choose] to select an arm, plays it externally, then reports the
 * observed reward via [update].
 *
 * Implementations source all randomness from [Bandit.random] so callers
 * control the PRNG.
 */
interface UnivariateBandit : Bandit {
    /** Pick an arm to play next; uses [Bandit.random] for any sampling. */
    fun choose(): Int

    /** Fold a single observed reward [value] (with optional [weight]) into the arm at [armIndex]. */
    fun update(armIndex: Int, value: Double, weight: Double = 1.0)

    /** Batched [update]: applies one observation per arm/value pair. */
    fun updateAll(armIndices: IntArray, values: DoubleArray, weights: DoubleArray? = null) {
        require(armIndices.size == values.size) { "armIndices and values must have equal size" }
        require(weights == null || weights.size == values.size) { "weights must match values size" }
        for (i in armIndices.indices) update(armIndices[i], values[i], weights?.get(i) ?: 1.0)
    }
}

/**
 * Context-aware bandit: each round the caller observes a feature vector
 * `x`, calls [choose] to pick an arm, plays it externally, observes a
 * reward, and feeds the `(x, reward)` pair back via [update].
 *
 * Implementations source all randomness from [Bandit.random].
 */
interface ContextualBandit : Bandit {
    /** Pick an arm to play next, given the per-round context [x]. */
    fun choose(x: VectorView): Int

    /** Fold a single `(x, reward)` observation (with optional [weight]) into the arm at [armIndex]. */
    fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double = 1.0)
}

/**
 * State surface for any bandit whose state can be checkpointed, replicated,
 * and merged with a sibling's. Independent of the action surface — every
 * bandit family has its own natural [S] (typically `List<R>` for per-arm
 * sufficient statistics; see [PerArmBandit]).
 */
interface Snapshotable<S> {
    /** Materialise the current state for inspection, serialisation, or replica merge. */
    fun snapshot(): S

    /** Merge [other]'s state into this one. Used to combine bandit replicas trained in parallel. */
    fun merge(other: S)

    /** Spawn a fresh bandit with the same configuration; state resets to the prior seed.
     *  The [random] source may be replaced (default: this bandit's [Bandit.random]). */
    fun create(random: Random): Snapshotable<S>
}

/**
 * Convenience for the dominant case where state is one [Result] per arm.
 * Adds per-arm access on top of [Snapshotable].
 */
interface PerArmBandit<R : Result> : Snapshotable<List<R>> {
    /** Per-arm snapshot at [armIndex]; default reads from [snapshot]. Implementations
     *  may override to avoid building the full list when only one arm is needed. */
    fun armResult(armIndex: Int): R = snapshot()[armIndex]
}

/**
 * Optional per-arm scoring for inspection / debugging / custom selectors.
 * Bandits whose [UnivariateBandit.choose] is an argmax over independent
 * per-arm scores expose this. Joint-sampling bandits (Boltzmann, Top-Two
 * Thompson) and exponential-weights bandits (Exp3) do not.
 */
interface Scorable {
    /** Score the arm at [armIndex] under the bandit's current state. */
    fun evaluate(armIndex: Int): Double
}

/** Contextual analog of [Scorable]: per-arm score under the current context. */
interface ContextualScorable {
    /** Score the arm at [armIndex] under the current state and context [x]. */
    fun evaluate(armIndex: Int, x: VectorView): Double
}
