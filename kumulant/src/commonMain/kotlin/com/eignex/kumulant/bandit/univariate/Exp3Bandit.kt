package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.MIN_PLAY_PROB
import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.bandit.renormaliseExponentialWeights
import com.eignex.kumulant.bandit.requireArmIndex
import com.eignex.kumulant.bandit.requireMergeSize
import com.eignex.kumulant.bandit.requireNbrArms
import com.eignex.kumulant.bandit.sampleFromDistribution
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.isInertWeight
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.random.Random

/** Per-arm snapshot for [Exp3Bandit]: the exponential-weight cell for one arm. */
@Serializable
@SerialName("Exp3ArmResult")
data class Exp3ArmResult(
    /** Unnormalised exponential weight `exp(eta * cumulative gain)`. */
    val weight: Double,
) : Result

/**
 * EXP3 (Auer, Cesa-Bianchi, Freund, Schapire 2002); adversarial multi-armed
 * bandit over a fixed pool of [nbrArms]. Each round: compute play
 * distribution `p[a] = (1 - gamma) · w[a]/Σw + gamma/K`, sample `a ~ p`, then
 * on reward `r ∈ [0,1]` update `w[a] *= exp(eta · r / p[a])` using the
 * importance-sampling-corrected gain.
 *
 * Regret bound is `O(sqrt(T · K · ln K))` under default tunings. Univariate
 * sibling to [com.eignex.kumulant.bandit.contextual.Exp4Bandit]; same
 * machinery without the expert layer. Standalone class (not a
 * [BanditPolicy] under [MultiArmedBandit]) because its sampling distribution
 * is computed across arms, not by independent-per-arm score + argmax.
 *
 * Rewards passed to [update] must lie in `[0, 1]` for the regret theory to
 * apply; outside-bound rewards are accepted but may destabilise the
 * exponential weight update.
 *
 * **Use cases:** non-stationary or adversarial scalar-reward problems where
 * the per-arm reward distribution may shift over time; settings where a
 * regret bound is wanted without distributional assumptions.
 *
 * **Arms:** indexless, `nbrArms` fixed at construction; per-arm state is one
 * exponential weight.
 *
 * **Memory:** O(nbrArms); one weight per arm plus a cached play
 * distribution.
 *
 * **Choose:** O(nbrArms); build the play distribution, inverse-CDF sample.
 *
 * **Update:** O(nbrArms); rebuilds the play distribution to read `p[arm]`,
 * then multiplicative weight update on the played arm.
 *
 * **Randomness:** every `choose` consumes one `random.nextDouble()`;
 * reproducible under a fixed seed.
 *
 * **Concurrency:** not thread-safe; weights and the cached play
 * distribution are mutated without synchronisation. Serialise `choose` and
 * `update` externally for multi-thread use.
 */
class Exp3Bandit(
    /** Number of arms. */
    override val nbrArms: Int,
    /** Learning rate on per-arm gain updates. */
    val eta: Double = defaultEta(nbrArms),
    /** Exploration mix: probability mass distributed uniformly across arms. */
    val gamma: Double = defaultGamma(eta),
    /** Single source of randomness. */
    override val random: Random = Random.Default,
) : UnivariateBandit,
    PerArmBandit<Exp3ArmResult> {
    init {
        requireNbrArms(nbrArms)
        require(eta > 0.0) { "eta must be positive, got $eta" }
        require(gamma in 0.0..1.0) { "gamma must lie in [0, 1], got $gamma" }
    }

    private val weights: DoubleArray = DoubleArray(nbrArms) { 1.0 }

    // The probability each arm was last played with, and how many of its pulls are still awaiting
    // feedback. The importance weight has to divide by the probability in force when the arm was
    // drawn, and the play distribution moves on every update, so recovering it at update time is
    // only correct when no other arm's feedback arrived in between.
    private val pendingPropensity = DoubleArray(nbrArms)
    private val pendingPulls = IntArray(nbrArms)

    /** Build the round's play distribution and sample an arm. */
    override fun choose(): Int {
        val p = playDistribution()
        val chosen = random.sampleFromDistribution(p)
        pendingPropensity[chosen] = p[chosen]
        pendingPulls[chosen]++
        return chosen
    }

    /** Current play distribution: weight-normalised softmax blended with uniform [gamma]. */
    fun playDistribution(): DoubleArray {
        var sumW = 0.0
        for (w in weights) sumW += w
        // A NaN reward, a run of large negative rewards that underflows every weight to zero, or a
        // merge of all-zero arm results leaves nothing to normalise by, and `w / sumW` would make
        // the whole distribution NaN - which `choose` then resolves to "always the last arm".
        // Uniform is the honest distribution when no arm has usable evidence.
        if (!(sumW > 0.0) || !sumW.isFinite()) return DoubleArray(nbrArms) { 1.0 / nbrArms }
        val out = DoubleArray(nbrArms)
        val uniform = gamma / nbrArms
        for (a in 0 until nbrArms) {
            val w = weights[a]
            out[a] = (1.0 - gamma) * (if (w.isFinite()) w / sumW else 0.0) + uniform
        }
        return out
    }

    /** Fold a `(arm, reward)` observation into the played arm's weight. */
    override fun update(armIndex: Int, value: Double, weight: Double) {
        requireArmIndex(armIndex, nbrArms)
        // Return before propensityOf, which consumes an outstanding pull. The exponential weights are
        // already untouched by a zero gain, but spending the recorded propensity is not a no-op: the
        // real feedback for that pull would then fall back to the current distribution and be divided
        // by the wrong probability, which is an unbounded error in the importance weight. See Stat.
        if (weight.isInertWeight()) return
        val p = propensityOf(armIndex).coerceAtLeast(MIN_PLAY_PROB)
        val gain = (value * weight) / p
        weights[armIndex] *= exp(eta * gain)
        weights.renormaliseExponentialWeights()
    }

    /**
     * Probability [armIndex] was drawn with, consuming one outstanding pull.
     *
     * Falls back to the current distribution when the caller updates an arm it never chose, which is
     * the best available estimate and what an off-policy caller implicitly asks for.
     */
    private fun propensityOf(armIndex: Int): Double {
        if (pendingPulls[armIndex] <= 0) return playDistribution()[armIndex]
        pendingPulls[armIndex]--
        return pendingPropensity[armIndex]
    }

    /** Current per-arm weights, normalised to sum to 1. */
    fun armWeights(): DoubleArray {
        val out = weights.copyOf()
        val s = out.sum()
        if (s > 0.0) for (i in out.indices) out[i] /= s
        return out
    }

    override fun snapshot(): List<Exp3ArmResult> = List(nbrArms) { Exp3ArmResult(weights[it]) }

    override fun merge(other: List<Exp3ArmResult>) {
        requireMergeSize(other.size, nbrArms)
        // No canonical merge for EXP3 weights; multiply elementwise as a coarse pool,
        // then renormalise. Use for "roughly combine two parallel runs", not principled
        // aggregation.
        for (i in 0 until nbrArms) weights[i] *= other[i].weight
        weights.renormaliseExponentialWeights()
    }

    /** Reset all weights to uniform. */
    override fun reset() {
        for (i in weights.indices) weights[i] = 1.0
        pendingPropensity.fill(0.0)
        pendingPulls.fill(0)
    }

    /** Spawn a fresh bandit with the same tunables; weights reset. */
    override fun create(random: Random): Exp3Bandit = Exp3Bandit(nbrArms, eta, gamma, random)

    /** EXP3 tuning defaults from Auer et al. */
    companion object {
        /** Horizon-free default `eta = sqrt(ln(K) / K)`. */
        fun defaultEta(nbrArms: Int): Double = sqrt(ln(nbrArms.toDouble().coerceAtLeast(2.0)) / nbrArms)

        /**
         * Horizon-free default exploration mix `min(1, eta)`.
         *
         * The textbook mixing rule `min(1, K * eta)` holds only when `eta` is the horizon-scaled
         * `sqrt(ln K / (K T))`. Against the horizon-free `eta` above, `K * eta` is `sqrt(K * ln K)`,
         * at least 1.177 for every `K >= 2`, so it would clamp to exactly 1.0 at every arm count and
         * collapse [playDistribution] to uniform. Using `eta` itself keeps the same "explore less as
         * evidence per arm grows" shape and tops out at 0.59 for two arms, so it cannot saturate.
         * Pass `gamma = 1.0` explicitly for pure uniform sampling.
         */
        fun defaultGamma(eta: Double): Double = eta.coerceAtMost(1.0)
    }
}
