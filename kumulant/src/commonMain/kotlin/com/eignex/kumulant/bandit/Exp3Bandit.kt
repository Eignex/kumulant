package com.eignex.kumulant.bandit

import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * EXP3 (Auer, Cesa-Bianchi, Freund, Schapire 2002) — adversarial multi-armed bandit
 * over a fixed pool of [nbrArms]. Each round:
 *
 *  1. Compute play distribution `p[a] = (1 - gamma) * w[a] / sum(w) + gamma / K`.
 *  2. Sample arm `a ~ p`.
 *  3. On reward `r in [0, 1]`, IPS-estimate gain `g = r / p[a]`, update
 *     `w[a] *= exp(eta * g)`.
 *
 * Regret bound is `O(sqrt(T * K * ln(K)))` under default tunings. Univariate sibling
 * to [Exp4Bandit] — same machinery without the expert layer. Standalone class because
 * its sampling distribution is computed across arms, not by independent-per-arm
 * `evaluate` + argmax.
 *
 * Rewards passed to [update] must lie in `[0, 1]` for the regret theory to apply;
 * outside-bound rewards are accepted but may destabilise the exponential weight update.
 */
class Exp3Bandit(
    /** Number of arms. */
    val nbrArms: Int,
    /** Learning rate on per-arm gain updates. */
    val eta: Double = defaultEta(nbrArms),
    /** Exploration mix: probability mass distributed uniformly across arms. */
    val gamma: Double = (nbrArms * eta).coerceAtMost(1.0),
    /** Single source of randomness. */
    val random: Random = Random.Default,
) {
    init {
        require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
        require(eta > 0.0) { "eta must be positive, got $eta" }
        require(gamma in 0.0..1.0) { "gamma must lie in [0, 1], got $gamma" }
    }

    private val weights: DoubleArray = DoubleArray(nbrArms) { 1.0 }
    private var lastPlayDist: DoubleArray = DoubleArray(nbrArms) { 1.0 / nbrArms }

    /** Build the round's play distribution and sample an arm. */
    fun choose(): Int {
        val p = playDistribution()
        lastPlayDist = p
        var u = random.nextDouble()
        for (a in 0 until nbrArms) {
            u -= p[a]
            if (u <= 0.0) return a
        }
        return nbrArms - 1
    }

    /** Current play distribution: weight-normalised softmax blended with uniform [gamma]. */
    fun playDistribution(): DoubleArray {
        var sumW = 0.0
        for (w in weights) sumW += w
        val out = DoubleArray(nbrArms)
        val uniform = gamma / nbrArms
        for (a in 0 until nbrArms) out[a] = (1.0 - gamma) * (weights[a] / sumW) + uniform
        return out
    }

    /** Fold a `(arm, reward)` observation into the played arm's weight. */
    fun update(armIndex: Int, reward: Double) {
        require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
        playDistribution().also { lastPlayDist = it }
        val p = lastPlayDist[armIndex].coerceAtLeast(MIN_PROB)
        val gain = reward / p
        weights[armIndex] *= exp(eta * gain)
        renormaliseIfNeeded()
    }

    /** Current per-arm weights, normalised to sum to 1. */
    fun armWeights(): DoubleArray {
        val out = weights.copyOf()
        val s = out.sum()
        if (s > 0.0) for (i in out.indices) out[i] /= s
        return out
    }

    /** Reset all weights to uniform. */
    fun reset() {
        for (i in weights.indices) weights[i] = 1.0
        lastPlayDist = DoubleArray(nbrArms) { 1.0 / nbrArms }
    }

    /** Spawn a fresh bandit with the same tunables; weights reset. */
    fun create(random: Random = this.random): Exp3Bandit =
        Exp3Bandit(nbrArms, eta, gamma, random)

    private fun renormaliseIfNeeded() {
        var maxW = 0.0
        for (w in weights) if (w > maxW) maxW = w
        if (maxW > RENORM_THRESHOLD) for (i in weights.indices) weights[i] /= maxW
    }

    /** EXP3 tuning defaults from Auer et al. */
    companion object {
        private const val MIN_PROB = 1e-12
        private const val RENORM_THRESHOLD = 1e100

        /** Horizon-free default `eta = sqrt(ln(K) / K)`. */
        fun defaultEta(nbrArms: Int): Double =
            sqrt(ln(nbrArms.toDouble().coerceAtLeast(2.0)) / nbrArms)
    }
}
