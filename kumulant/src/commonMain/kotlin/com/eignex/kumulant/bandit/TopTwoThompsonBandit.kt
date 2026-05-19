package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.random.Random

/**
 * Top-Two Thompson Sampling (Russo 2020) — pure-exploration variant of Thompson
 * sampling targeting best-arm identification rather than regret minimisation. Each
 * round:
 *
 *  1. Sample once from every arm's posterior; let `arm1 = argmax(scores)`.
 *  2. With probability [beta], play `arm1`.
 *  3. Otherwise resample posteriors until the argmax differs from `arm1`; play that
 *     second-favoured arm (`arm2`).
 *
 * The forced resampling keeps a fraction of the budget on the runner-up so the gap to
 * the best arm is identified asymptotically optimally. Converges to the optimal
 * exploration fraction `beta = 0.5` for two-armed problems; tune lower to bias toward
 * exploitation when running in the regret-minimisation regime.
 *
 * Standalone bandit because Top-Two needs to break out of "independent per-arm score,
 * then argmax" — it samples the population *jointly* and conditionally resamples.
 */
class TopTwoThompsonBandit<R : Result>(
    /** Number of arms. */
    val nbrArms: Int,
    /** Per-arm posterior + arm spec. */
    val policy: ThompsonSampling<R>,
    /** Probability of playing the round's top sample; default 0.5 is Russo's recipe. */
    val beta: Double = 0.5,
    /** Cap on the resample loop when searching for a different second arm. */
    val maxResamples: Int = 32,
    /** Single source of randomness. */
    val random: Random = Random.Default,
) {
    init {
        require(nbrArms >= 2) { "nbrArms must be at least 2 for top-two, got $nbrArms" }
        require(beta in 0.0..1.0) { "beta must lie in [0, 1], got $beta" }
        require(maxResamples > 0) { "maxResamples must be positive, got $maxResamples" }
    }

    private val stats: Array<SeriesStat<R>> = Array(nbrArms) { policy.createArm() }
    private var step: Long = 0L

    /** Choose an arm via the top-two protocol. */
    fun choose(): Int {
        val arm1 = sampleArgmax()
        if (random.nextDouble() < beta) return arm1
        repeat(maxResamples) {
            val candidate = sampleArgmax()
            if (candidate != arm1) return candidate
        }
        return arm1
    }

    /** Single posterior sample per arm; return the argmax. */
    fun sampleArgmax(): Int {
        val t = step++
        var bestIdx = 0
        var bestScore = Double.NEGATIVE_INFINITY
        for (a in 0 until nbrArms) {
            val s = policy.evaluate(stats[a].read(0L), t, random)
            if (s > bestScore) {
                bestScore = s
                bestIdx = a
            }
        }
        return bestIdx
    }

    /** Fold a reward observation into arm [armIndex]. */
    fun update(armIndex: Int, value: Double, weight: Double = 1.0) {
        require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
        policy.update(stats[armIndex], value, weight)
    }

    /** Snapshot every arm. */
    fun snapshot(): List<R> = stats.map { it.read(0L) }

    /** Per-arm snapshot at [armIndex]. */
    fun armResult(armIndex: Int): R = stats[armIndex].read(0L)

    /** Reset every arm to its prior. */
    fun reset() {
        for (i in 0 until nbrArms) stats[i] = policy.createArm()
        step = 0L
    }

    /** Merge each `others[i]` into the corresponding arm. */
    fun merge(others: List<R>) {
        require(others.size == nbrArms) {
            "merge: others.size=${others.size} does not match nbrArms=$nbrArms"
        }
        for (i in 0 until nbrArms) stats[i].merge(others[i])
    }

    /** Spawn a fresh replica. */
    fun create(random: Random = this.random): TopTwoThompsonBandit<R> =
        TopTwoThompsonBandit(nbrArms, policy, beta, maxResamples, random)
}
