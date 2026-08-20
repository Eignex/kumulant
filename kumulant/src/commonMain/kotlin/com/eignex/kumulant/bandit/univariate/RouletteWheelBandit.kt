package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.Scorable
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.bandit.requireArmIndex
import com.eignex.kumulant.bandit.requireMergeSize
import com.eignex.kumulant.bandit.requireNbrArms
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.isInertWeight
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.random.Random

/**
 * Per-arm state snapshot for [RouletteWheelBandit]. Exposes the current weight plus the
 * running segment counters callers may want to inspect for debugging.
 */
@Serializable
@SerialName("RouletteWheelArmResult")
data class RouletteWheelArmResult(
    /** Current arm weight used by the roulette draw. */
    val weight: Double,
    /** Sum of rewards observed for this arm since the last segment rebalance. */
    val accumulatedScore: Double,
    /** Total observation weight recorded for this arm since the last segment rebalance. */
    val totalWeights: Double,
) : Result

/**
 * Adaptive operator-selection bandit in the Ropke-Pisinger 2006 ALNS scheme:
 * each arm carries a weight, `choose` samples proportional to weights
 * (roulette wheel), and weights re-balance in batches.
 *
 * After every [segmentLength] [update] calls, weights re-balance via
 * `w_i = w_i · (1 - reactionFactor) + reactionFactor · avgScore_i`, where
 * `avgScore_i` is the weight-normalised mean reward of arm `i` over the segment,
 * floored at [minWeight] so no arm is permanently extinguished. Batched
 * re-balance (rather than per-observation) is useful when rewards are noisy
 * and continuous updates would thrash. Only meaningful for
 * reward-maximisation: the weight increase is asymmetric and has no clean
 * "minimise" dual; callers wanting to minimise should negate the reward
 * before [update].
 *
 * Implemented as a direct [UnivariateBandit] rather than a [BanditPolicy]
 * plugged into [MultiArmedBandit] because the re-balance is a global
 * cross-arm operation (each arm's new weight depends on its segment-mean).
 *
 * **Use cases:** operator selection inside meta-heuristics (ALNS, LNS),
 * where reward signals are noisy and selection breadth across many candidate
 * operators matters more than fine-grained per-step tracking.
 *
 * **Arms:** indexless, `nbrArms` fixed at construction; per-arm state is
 * `(weight, segment score sum, segment weight sum)`.
 *
 * **Memory:** O(nbrArms); three parallel arrays plus a segment counter.
 *
 * **Choose:** O(nbrArms); sum the weights, inverse-CDF sample.
 *
 * **Update:** O(1) amortised; O(nbrArms) on the segment boundary where the
 * re-balance sweeps every arm.
 *
 * **Randomness:** every `choose` consumes one `random.nextDouble()` (or one
 * `nextInt` when all weights collapse to zero); reproducible under a fixed
 * seed.
 *
 * **Concurrency:** not thread-safe; weights, segment scores, and the
 * segment counter are mutated without synchronisation. Serialise `choose`
 * and `update` externally for multi-thread use.
 */
class RouletteWheelBandit(
    /** Number of arms in the population. */
    override val nbrArms: Int,
    /** Blend factor for the Ropke-Pisinger weight update; 0 = no learning, 1 = pure segment-mean. */
    val reactionFactor: Double = 0.1,
    /** Number of [update] calls between successive weight rebalances. */
    val segmentLength: Int = 10,
    /** Starting weight assigned to every arm. */
    val initialWeight: Double = 1.0,
    /** Floor on the rebalanced weight; prevents arms from being permanently extinguished. */
    val minWeight: Double = 0.01,
    override val random: Random = Random.Default,
) : UnivariateBandit,
    PerArmBandit<RouletteWheelArmResult>,
    Scorable {

    init {
        requireNbrArms(nbrArms)
        require(reactionFactor in 0.0..1.0) { "reactionFactor must be in [0, 1], got $reactionFactor" }
        require(segmentLength > 0) { "segmentLength must be positive, got $segmentLength" }
        require(minWeight > 0.0) { "minWeight must be positive, got $minWeight" }
    }

    private val weights = DoubleArray(nbrArms) { initialWeight }
    private val accumulatedScores = DoubleArray(nbrArms)
    private val segmentWeights = DoubleArray(nbrArms)
    private var picksThisSegment = 0

    override fun choose(): Int {
        var total = 0.0
        for (w in weights) total += w
        if (total <= 0.0) return random.nextInt(nbrArms)
        var draw = random.nextDouble() * total
        for (i in weights.indices) {
            draw -= weights[i]
            if (draw <= 0.0) return i
        }
        return nbrArms - 1
    }

    /** The arm's current weight; `choose` computes the same quantity inline. */
    override fun evaluate(armIndex: Int): Double {
        requireArmIndex(armIndex, nbrArms)
        return weights[armIndex]
    }

    override fun update(armIndex: Int, value: Double, weight: Double) {
        requireArmIndex(armIndex, nbrArms)
        // Return before the counter and the segment clock, not just before the score: advancing them
        // on a zero-weight observation would dilute the arm's average and could trip a rebalance.
        // Zero weight means "ignore this observation" library-wide.
        if (weight.isInertWeight()) return
        accumulatedScores[armIndex] += value * weight
        segmentWeights[armIndex] += weight
        picksThisSegment++
        if (picksThisSegment >= segmentLength) {
            rebalance()
            picksThisSegment = 0
        }
    }

    private fun rebalance() {
        for (i in weights.indices) {
            if (segmentWeights[i] > 0.0) {
                val avg = accumulatedScores[i] / segmentWeights[i]
                val blended = weights[i] * (1.0 - reactionFactor) + reactionFactor * avg
                // coerceAtLeast is `if (this < min) min else this`, and NaN < minWeight is false, so a
                // NaN blend walks straight through the floor that exists to keep weights usable. It
                // then makes the roulette total NaN, and `choose` resolves that to "always the last
                // arm" for good. An arm with no usable evidence gets the floor, like any other.
                weights[i] = if (blended.isFinite()) blended.coerceAtLeast(minWeight) else minWeight
            }
            accumulatedScores[i] = 0.0
            segmentWeights[i] = 0.0
        }
    }

    override fun snapshot(): List<RouletteWheelArmResult> =
        List(nbrArms) { RouletteWheelArmResult(weights[it], accumulatedScores[it], segmentWeights[it]) }

    /**
     * Heuristic merge: weights are arithmetically averaged across replicas; scores and
     * segment weights are summed (treating the other replica's segment as additional
     * unobserved data). Roulette-wheel adaptive selection has no canonical merge
     * semantics - the segment-based rebalance is inherently sequential - so use this
     * for "roughly combine two parallel runs" rather than for principled aggregation.
     */
    override fun merge(other: List<RouletteWheelArmResult>) {
        requireMergeSize(other.size, nbrArms)
        for (i in 0 until nbrArms) {
            weights[i] = ((weights[i] + other[i].weight) / 2.0).coerceAtLeast(minWeight)
            accumulatedScores[i] += other[i].accumulatedScore
            segmentWeights[i] += other[i].totalWeights
        }
    }

    override fun reset() {
        for (i in 0 until nbrArms) {
            weights[i] = initialWeight
            accumulatedScores[i] = 0.0
            segmentWeights[i] = 0.0
        }
        picksThisSegment = 0
    }

    override fun create(random: Random): RouletteWheelBandit =
        RouletteWheelBandit(nbrArms, reactionFactor, segmentLength, initialWeight, minWeight, random)
}
