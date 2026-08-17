package com.eignex.kumulant.bandit.contextual

import com.eignex.koblas.VectorView
import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.MIN_PLAY_PROB
import com.eignex.kumulant.bandit.Snapshotable
import com.eignex.kumulant.bandit.renormaliseExponentialWeights
import com.eignex.kumulant.bandit.requireArmIndex
import com.eignex.kumulant.bandit.requireNbrArms
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.preview
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Snapshot of [com.eignex.kumulant.bandit.contextual.Exp4Bandit]'s state: the per-expert exponential weights. The
 * bandit's state is over experts (not arms), so it surfaces via [Snapshotable]
 * rather than the [com.eignex.kumulant.bandit.PerArmBandit] per-arm convenience.
 */
@Serializable
@SerialName("Exp4State")
data class Exp4State(
    /** Unnormalised per-expert weights; element `i` is `exp(eta · cumulative gain_i)`. */
    val weights: DoubleArray,
) : Result {
    override fun equals(other: Any?): Boolean = other is Exp4State &&
        weights.contentEquals(other.weights)

    override fun hashCode(): Int = weights.contentHashCode()

    override fun toString(): String = "Exp4State(weights=${weights.preview()})"
}

/**
 * Maps a context vector to a probability distribution over arms. Implementations are
 * stateless w.r.t. the bandit; they consult only the context and any internal state
 * frozen at construction. The returned array must have length `nbrArms` and sum to 1.
 */
fun interface Exp4Expert {
    /** Distribution over arms for [x]. Result must sum to 1 and have length `nbrArms`. */
    fun advise(x: VectorView, nbrArms: Int): DoubleArray
}

/**
 * EXP4 (Auer, Cesa-Bianchi, Freund, Schapire 2002); adversarial contextual
 * bandit over a fixed pool of [experts]. Each round, every expert returns a
 * distribution over arms for the context; the bandit mixes those
 * distributions weighted by per-expert exponential weights, blends with
 * uniform exploration `gamma`, samples an arm, and on reward `r ∈ [0,1]`
 * folds the IPS-corrected gain back into the expert weights.
 *
 * Regret bound is `O(sqrt(T · K · ln N))` under the default [eta]/[gamma]
 * picks derived from `nbrArms` (K) and `experts.size` (N), so the algorithm
 * trades off exploration breadth (more experts) against learning rate.
 * Rewards passed to [update] must lie in `[0, 1]` for the regret theory to
 * apply; outside-bound rewards are accepted but may destabilise the weight
 * updates.
 *
 * State is per-expert (not per-arm) so it surfaces via
 * [Snapshotable]&lt;[Exp4State]&gt; rather than the
 * [com.eignex.kumulant.bandit.PerArmBandit] convenience used by sibling
 * contextual bandits.
 *
 * **Use cases:** non-stationary or adversarial contextual problems where a
 * small set of policies (linear scorers, rule-based heuristics, pretrained
 * models) can advise arm distributions; meta-learning over a finite pool of
 * experts.
 *
 * **Arms:** contextual with caller-defined feature dimension (every
 * expert's `advise` returns length `nbrArms`); `nbrArms` and `experts.size`
 * fixed at construction.
 *
 * **Memory:** O(experts.size + experts.size · nbrArms); one weight per
 * expert plus a cached last-advice matrix and play distribution.
 *
 * **Choose:** O(experts.size · (advise + nbrArms)); query every expert and
 * mix their distributions.
 *
 * **Update:** O(experts.size · (advise + nbrArms)); re-evaluates experts at
 * `x` so the played arm's IPS gain is correct, then multiplicative update
 * across all expert weights.
 *
 * **Randomness:** every `choose` consumes one `random.nextDouble()`;
 * reproducible under a fixed seed when expert `advise` is deterministic.
 *
 * **Concurrency:** not thread-safe; expert weights, the cached advice
 * matrix, and the cached play distribution are mutated without
 * synchronisation. Serialise `choose` and `update` externally for
 * multi-thread use.
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
     *  blending in the expert mixture. Defaults to `min(1, eta)`; see [defaultGamma]. */
    val gamma: Double = defaultGamma(eta),
    /** Single source of randomness for the round's arm draw. */
    override val random: Random = Random.Default,
) : ContextualBandit,
    Snapshotable<Exp4State> {
    init {
        requireNbrArms(nbrArms)
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
        // Nothing to normalise by once every expert weight has underflowed to zero (or one has gone
        // NaN): `w / wSum` would make the whole distribution NaN, which `choose` resolves to
        // "always the last arm". Fall back to the unweighted expert mean, which still uses the
        // advice even though the experts' relative standing has been lost.
        val usable = wSum > 0.0 && wSum.isFinite()
        val out = DoubleArray(nbrArms)
        val uniform = gamma / nbrArms
        for (a in 0 until nbrArms) {
            var s = 0.0
            for (i in experts.indices) {
                val share = if (usable) weights[i] / wSum else 1.0 / experts.size
                s += (if (share.isFinite()) share else 0.0) * lastAdvice[i][a]
            }
            out[a] = (1.0 - gamma) * s + uniform
        }
        return out
    }

    /** Fold a `(context, reward)` observation back into the expert weights. */
    override fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double) {
        requireArmIndex(armIndex, nbrArms)
        // Re-evaluate experts in case caller calls update without a prior choose at this x.
        playDistribution(x)
        val pPlayed = lastPlayDist[armIndex].coerceAtLeast(MIN_PLAY_PROB)
        val gainPlayed = reward / pPlayed
        for (i in experts.indices) {
            val expertGain = lastAdvice[i][armIndex] * gainPlayed
            weights[i] *= exp(eta * expertGain)
        }
        weights.renormaliseExponentialWeights()
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
        // No canonical merge for EXP4 expert weights; multiply elementwise as a coarse
        // pool, then renormalise. Use for "roughly combine two parallel runs", not
        // principled aggregation.
        for (i in weights.indices) weights[i] *= other.weights[i]
        weights.renormaliseExponentialWeights()
    }

    /** Reset all expert weights to uniform. */
    override fun reset() {
        for (i in weights.indices) weights[i] = 1.0
        lastPlayDist = DoubleArray(nbrArms) { 1.0 / nbrArms }
    }

    /** Spawn a fresh bandit with the same experts and tunables; weights reset to uniform. */
    override fun create(random: Random): Exp4Bandit = Exp4Bandit(nbrArms, experts, eta, gamma, random)

    /** Default-tuning helpers. */
    companion object {
        /** Default learning rate from the EXP4 regret analysis: `sqrt(ln(N) / (T * K))`
         *  collapsed to a horizon-free form using `T = 1` as a starting heuristic. */
        fun defaultEta(nbrArms: Int, nbrExperts: Int): Double =
            sqrt(ln(nbrExperts.toDouble().coerceAtLeast(2.0)) / nbrArms)

        /**
         * Default exploration mix `min(1, eta)`.
         *
         * This used to be `min(1, K * eta)`, which with the horizon-free [defaultEta] above works
         * out to `sqrt(K * ln N)` - at least 1.177 for any real configuration, so it clamped to
         * exactly 1.0 and [playDistribution] became uniform, multiplying the expert mixture by
         * `1 - gamma == 0`. `K * eta` is only the textbook rule when `eta` still carries the `1/T`
         * horizon term. Pass `gamma = 1.0` explicitly for pure uniform sampling.
         */
        fun defaultGamma(eta: Double): Double = eta.coerceAtMost(1.0)
    }
}
