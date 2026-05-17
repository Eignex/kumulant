package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
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
    val weight: Double,
    val accumulatedScore: Double,
    val callCount: Int,
) : Result

/**
 * Adaptive operator-selection bandit in the Ropke-Pisinger 2006 ALNS scheme. Each arm
 * has a weight; [choose] samples arms with probability proportional to their weights
 * (roulette wheel). After every [segmentLength] [update] calls, weights re-balance via
 *
 *     w_i = w_i * (1 - reactionFactor) + reactionFactor * avgScore_i
 *
 * where `avgScore_i` is the mean reward per call of arm i over the segment, floored at
 * [minWeight] so no arm gets permanently extinguished. Compared to the policy-driven
 * [MultiArmedBandit], this scheme batches the weight update over a segment of picks
 * rather than reacting per observation - useful when rewards are noisy and continuous
 * updates would thrash.
 *
 * Unlike most kumulant bandits, the weight rebalance is a global cross-arm operation
 * (each arm's new weight depends on its segment-mean), so it's implemented as a direct
 * [UnivariateBandit] rather than a [BanditPolicy] plugged into [MultiArmedBandit].
 *
 * Only meaningful for reward-maximisation: Ropke-Pisinger's weight increase is
 * asymmetric and has no clean "minimize" dual. Callers wanting to minimise should
 * negate the reward before calling [update].
 */
class RouletteWheelBandit(
    val nbrArms: Int,
    val reactionFactor: Double = 0.1,
    val segmentLength: Int = 10,
    val initialWeight: Double = 1.0,
    val minWeight: Double = 0.01,
    override val random: Random = Random.Default,
) : UnivariateBandit<RouletteWheelArmResult> {

    init {
        require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
        require(reactionFactor in 0.0..1.0) { "reactionFactor must be in [0, 1], got $reactionFactor" }
        require(segmentLength > 0) { "segmentLength must be positive, got $segmentLength" }
        require(minWeight > 0.0) { "minWeight must be positive, got $minWeight" }
    }

    private val weights = DoubleArray(nbrArms) { initialWeight }
    private val accumulatedScores = DoubleArray(nbrArms)
    private val callCounts = IntArray(nbrArms)
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

    /** Returns the arm's current weight. Useful for inspection / debugging; not used by
     *  [choose] which computes the roulette draw inline. */
    override fun evaluate(armIndex: Int): Double = weights[armIndex]

    override fun update(armIndex: Int, value: Double, weight: Double) {
        accumulatedScores[armIndex] += value * weight
        callCounts[armIndex]++
        picksThisSegment++
        if (picksThisSegment >= segmentLength) {
            rebalance()
            picksThisSegment = 0
        }
    }

    private fun rebalance() {
        for (i in weights.indices) {
            if (callCounts[i] > 0) {
                val avg = accumulatedScores[i] / callCounts[i]
                weights[i] = (weights[i] * (1.0 - reactionFactor) + reactionFactor * avg)
                    .coerceAtLeast(minWeight)
            }
            accumulatedScores[i] = 0.0
            callCounts[i] = 0
        }
    }

    override fun snapshot(): List<RouletteWheelArmResult> =
        List(nbrArms) { RouletteWheelArmResult(weights[it], accumulatedScores[it], callCounts[it]) }
}
