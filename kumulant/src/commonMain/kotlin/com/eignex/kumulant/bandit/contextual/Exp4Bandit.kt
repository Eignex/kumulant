package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.Snapshotable
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.math.ln
import kotlin.random.Random

/**
 * Snapshot of [com.eignex.kumulant.bandit.contextual.Exp4Bandit]'s state: the per-expert exponential weights. The
 * bandit's state is over experts (not arms), so it surfaces via [Snapshotable]
 * rather than the [com.eignex.kumulant.bandit.PerArmBandit] per-arm convenience.
 */
@Serializable
@SerialName("Exp4State")
data class Exp4State(
    /** Unnormalised per-expert weights — element `i` is `exp(eta · cumulative gain_i)`. */
    val weights: DoubleArray,
) : Result

/**
 * Maps a context vector to a probability distribution over arms. Implementations are
 * stateless w.r.t. the bandit — they consult only the context and any internal state
 * frozen at construction. The returned array must have length `nbrArms` and sum to 1.
 */
fun interface Exp4Expert {
    /** Distribution over arms for [x]. Result must sum to 1 and have length `nbrArms`. */
    fun advise(x: VectorView, nbrArms: Int): DoubleArray
}

/**
 * EXP4 (Auer, Cesa-Bianchi, Freund, Schapire 2002) — adversarial contextual bandit
 * over a fixed pool of [experts]. Each round:
 *
 *  1. Each expert returns a distribution over arms given the context.
 *  2. The bandit mixes expert distributions weighted by the experts' current weights,
 *     then blends with uniform exploration (`gamma`) to form the play distribution `p`.
 *  3. Arm `a ~ p` is played; on reward `r in [0,1]`, the IPS-style estimated gain
 *     `r / p[a]` for the played arm (0 elsewhere) feeds back into per-expert weight
 *     updates `w_i *= exp(eta * xi_i[a] * r / p[a])`.
 *
 * Regret bound is `O(sqrt(T * K * ln(N)))` under default [eta]/[gamma] picks
 * derived from `nbrArms` (K) and `experts.size` (N), so the algorithm trades off
 * exploration breadth (more experts) against learning rate. Rewards passed to [update]
 * must lie in `[0, 1]` for the regret theory to apply; outside-bound rewards are
 * accepted but may destabilise the weight updates.
 *
 * Its state is per-expert (not per-arm) so it surfaces via
 * [Snapshotable]&lt;[Exp4State]&gt; rather than the [com.eignex.kumulant.bandit.PerArmBandit]
 * convenience used by sibling contextual bandits.
 */
class Exp4Bandit(
    /** Number of arms; every expert's distribution must have this length. */
    override val nbrArms: Int,
    /** Fixed pool of experts; non-empty. */
    val experts: List<Exp4Expert>,
    /** Learning rate on per-expert gain updates. Defaults to `sqrt(ln(N) / (T * K))`
     *  with `T = horizon`; pass a static value if the horizon is unknown. */
    val eta: Double = defaultEta(nbrArms, experts.size),
    /** Exploration mix: probability mass distributed uniformly across arms before
     *  blending in the expert mixture. Defaults to `K * eta`. */
    val gamma: Double = (nbrArms * eta).coerceAtMost(1.0),
    /** Single source of randomness for the round's arm draw. */
    override val random: Random = Random.Default,
) : ContextualBandit, Snapshotable<Exp4State> {
    init {
        require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
        require(experts.isNotEmpty()) { "experts must be non-empty" }
        require(eta > 0.0) { "eta must be positive, got $eta" }
        require(gamma in 0.0..1.0) { "gamma must lie in [0, 1], got $gamma" }
    }

    private val weights: DoubleArray = DoubleArray(experts.size) { 1.0 }
    private val lastAdvice: Array<DoubleArray> = Array(experts.size) { DoubleArray(nbrArms) }
    private var lastPlayDist: DoubleArray = DoubleArray(nbrArms) { 1.0 / nbrArms }

    /** Build the round's play distribution and sample an arm. */
    override fun choose(x: VectorView): Int {
        val p = playDistribution(x)
        lastPlayDist = p
        var u = random.nextDouble()
        for (a in 0 until nbrArms) {
            u -= p[a]
            if (u <= 0.0) return a
        }
        return nbrArms - 1
    }

    /** Mean of expert distributions at [x] weighted by current weights, blended with
     *  uniform exploration via [gamma]. */
    fun playDistribution(x: VectorView): DoubleArray {
        var wSum = 0.0
        for (i in experts.indices) {
            val xi = experts[i].advise(x, nbrArms)
            require(xi.size == nbrArms) { "expert $i returned ${xi.size} probs, expected $nbrArms" }
            lastAdvice[i] = xi
            wSum += weights[i]
        }
        val out = DoubleArray(nbrArms)
        val uniform = gamma / nbrArms
        for (a in 0 until nbrArms) {
            var s = 0.0
            for (i in experts.indices) s += (weights[i] / wSum) * lastAdvice[i][a]
            out[a] = (1.0 - gamma) * s + uniform
        }
        return out
    }

    /** Fold a `(context, reward)` observation back into the expert weights. */
    override fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double) {
        require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
        // Re-evaluate experts in case caller calls update without a prior choose at this x.
        playDistribution(x)
        val pPlayed = lastPlayDist[armIndex].coerceAtLeast(MIN_PROB)
        val gainPlayed = reward / pPlayed
        for (i in experts.indices) {
            val expertGain = lastAdvice[i][armIndex] * gainPlayed
            weights[i] *= exp(eta * expertGain)
        }
        normalizeIfNeeded()
    }

    /** Current per-expert weights, normalised to sum to 1. */
    fun expertWeights(): DoubleArray {
        val out = weights.copyOf()
        val s = out.sum()
        if (s > 0.0) for (i in out.indices) out[i] /= s
        return out
    }

    override fun snapshot(): Exp4State = Exp4State(weights.copyOf())

    override fun merge(other: Exp4State) {
        require(other.weights.size == experts.size) {
            "merge: state has ${other.weights.size} expert weights, expected ${experts.size}"
        }
        // No canonical merge for EXP4 expert weights — multiply elementwise as a coarse
        // pool, then renormalise. Use for "roughly combine two parallel runs", not
        // principled aggregation.
        for (i in weights.indices) weights[i] *= other.weights[i]
        normalizeIfNeeded()
    }

    /** Reset all expert weights to uniform. */
    override fun reset() {
        for (i in weights.indices) weights[i] = 1.0
        lastPlayDist = DoubleArray(nbrArms) { 1.0 / nbrArms }
    }

    /** Spawn a fresh bandit with the same experts and tunables; weights reset to uniform. */
    override fun create(random: Random): Exp4Bandit =
        Exp4Bandit(nbrArms, experts, eta, gamma, random)

    private fun normalizeIfNeeded() {
        var maxW = 0.0
        for (w in weights) if (w > maxW) maxW = w
        if (maxW > RENORM_THRESHOLD) {
            for (i in weights.indices) weights[i] /= maxW
        }
    }

    /** Default-tuning helpers. */
    companion object {
        private const val MIN_PROB = 1e-12
        private const val RENORM_THRESHOLD = 1e100

        /** Default learning rate from the EXP4 regret analysis: `sqrt(ln(N) / (T * K))`
         *  collapsed to a horizon-free form using `T = 1` as a starting heuristic. */
        fun defaultEta(nbrArms: Int, nbrExperts: Int): Double =
            kotlin.math.sqrt(ln(nbrExperts.toDouble().coerceAtLeast(2.0)) / nbrArms)
    }
}
