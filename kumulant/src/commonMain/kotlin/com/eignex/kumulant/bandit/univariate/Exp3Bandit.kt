package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.MIN_PLAY_PROB
import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.bandit.clampLogWeight
import com.eignex.kumulant.bandit.normaliseLogWeights
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
    /**
     * Log of the exponential weight, `eta * cumulative gain`, shifted so the largest arm sits at zero.
     */
    val logWeight: Double,
) : Result

/**
 * EXP3 (Auer, Cesa-Bianchi, Freund, Schapire 2002); adversarial multi-armed
 * bandit over a fixed pool of [nbrArms]. Each round: compute play
 * distribution `p(a) = (1 - gamma) * w(a) / sum(w) + gamma/K`, sample `a ~ p`, then
 * on reward `r ∈ [0,1]` update `w(a) *= exp(eta * r / p(a))` using the
 * importance-sampling-corrected gain. Weights are held as logs, so no gain overflows or
 * permanently zeroes an arm's weight.
 *
 * Computes a joint sampling distribution across arms, with regret bound
 * `O(sqrt(T · K · ln K))` under default tunings.
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
 * **Memory:** O(nbrArms); weights, pending play probabilities, and pending pull counts.
 *
 * **Choose:** O(nbrArms); build the play distribution, inverse-CDF sample.
 *
 * **Update:** O(nbrArms); additive update of the log weight and a shift so the largest is zero.
 * Uses the arm's most recently recorded play probability while feedback is pending,
 * otherwise recomputes the current distribution.
 *
 * **Randomness:** every `choose` consumes one `random.nextDouble()`;
 * reproducible under a fixed seed.
 *
 * **Concurrency:** not thread-safe; weights and the cached play
 * distribution are mutated without synchronisation. Serialise `choose` and
 * `update` externally for multi-thread use.
 */
class Exp3Bandit(
    override val nbrArms: Int,
    /** Learning rate on per-arm gain updates. */
    val eta: Double = defaultEta(nbrArms),
    /** Exploration mix: probability mass distributed uniformly across arms. */
    val gamma: Double = defaultGamma(eta),
    override val random: Random = Random.Default,
) : UnivariateBandit,
    PerArmBandit<Exp3ArmResult> {
    init {
        requireNbrArms(nbrArms)
        require(eta > 0.0) { "eta must be positive, got $eta" }
        require(gamma in 0.0..1.0) { "gamma must lie in [0, 1], got $gamma" }
    }

    private val logWeights: DoubleArray = DoubleArray(nbrArms)

    // Delayed feedback must use a recorded play probability because other updates move the distribution.
    // Repeated outstanding pulls of one arm share its most recently recorded probability.
    private val pendingPropensity = DoubleArray(nbrArms)
    private val pendingPulls = IntArray(nbrArms)

    override fun choose(): Int {
        val p = playDistribution()
        val chosen = random.sampleFromDistribution(p)
        pendingPropensity[chosen] = p[chosen]
        pendingPulls[chosen]++
        return chosen
    }

    /** Current play distribution: weight-normalised softmax blended with uniform [gamma]. */
    fun playDistribution(): DoubleArray {
        val w = relativeWeights()
        val sumW = w.sum()
        // Invalid totals cannot be normalised; uniform sampling keeps every arm reachable.
        if (!(sumW > 0.0) || !sumW.isFinite()) return DoubleArray(nbrArms) { 1.0 / nbrArms }
        val uniform = gamma / nbrArms
        for (a in 0 until nbrArms) w[a] = (1.0 - gamma) * (w[a] / sumW) + uniform
        return w
    }

    override fun update(armIndex: Int, value: Double, weight: Double) {
        requireArmIndex(armIndex, nbrArms)
        // Inert feedback must not consume a pending pull's recorded probability.
        if (weight.isInertWeight()) return
        val p = propensityOf(armIndex).coerceAtLeast(MIN_PLAY_PROB)
        val logGain = eta * (value * weight) / p
        // A NaN gain carries no evidence and would poison every later normalisation.
        if (logGain.isNaN()) return
        logWeights[armIndex] = (logWeights[armIndex] + logGain).clampLogWeight()
        logWeights.normaliseLogWeights()
    }

    // Off-policy updates without a pending pull use the current distribution as an estimate.
    private fun propensityOf(armIndex: Int): Double {
        if (pendingPulls[armIndex] <= 0) return playDistribution()[armIndex]
        pendingPulls[armIndex]--
        return pendingPropensity[armIndex]
    }

    /** Current per-arm weights, normalised to sum to 1. */
    fun armWeights(): DoubleArray {
        val out = relativeWeights()
        val s = out.sum()
        if (s > 0.0) for (i in out.indices) out[i] /= s
        return out
    }

    // The largest log weight is zero, so the largest relative weight is one and the sum cannot overflow.
    private fun relativeWeights(): DoubleArray = DoubleArray(nbrArms) { exp(logWeights[it]) }

    override fun snapshot(): List<Exp3ArmResult> = List(nbrArms) { Exp3ArmResult(logWeights[it]) }

    /**
     * Approximate pooling by multiplying corresponding arm weights, adding their logs.
     * Non-finite log weights carry no usable evidence and are skipped.
     * Pending play probabilities and pull counts are not included in the snapshot.
     */
    override fun merge(other: List<Exp3ArmResult>) {
        requireMergeSize(other.size, nbrArms)
        for (i in 0 until nbrArms) {
            val logWeight = other[i].logWeight
            if (logWeight.isFinite()) logWeights[i] = (logWeights[i] + logWeight).clampLogWeight()
        }
        logWeights.normaliseLogWeights()
    }

    override fun reset() {
        logWeights.fill(0.0)
        pendingPropensity.fill(0.0)
        pendingPulls.fill(0)
    }

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
