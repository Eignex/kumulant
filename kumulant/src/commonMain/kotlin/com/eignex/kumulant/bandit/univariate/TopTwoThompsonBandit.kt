package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.random.Random

/**
 * Top-Two Thompson Sampling (Russo 2020) — pure-exploration variant of
 * Thompson sampling for best-arm identification: sample every arm's
 * posterior, take the argmax `arm1`, play it with probability [beta], or
 * else resample until the argmax differs from `arm1` and play that runner-up.
 *
 * The forced resampling keeps a fraction of the budget on the runner-up so
 * the gap to the best arm is identified asymptotically optimally. Converges
 * to the optimal exploration fraction `beta = 0.5` for two-armed problems;
 * tune lower to bias toward exploitation when running in the
 * regret-minimisation regime.
 *
 * Doesn't expose [com.eignex.kumulant.bandit.Scorable]: arm selection
 * samples jointly and conditionally resamples, so there's no per-arm score
 * callers can read in isolation. The per-arm posterior state still fits
 * [PerArmBandit].
 *
 * **Use cases:** best-arm identification and pure-exploration problems where
 * shrinking the regret gap matters more than minimising cumulative regret;
 * any posterior expressible as a [ThompsonSampling] policy.
 *
 * **Arms:** indexless, `nbrArms ≥ 2` fixed at construction; each arm owns
 * one posterior cell from `policy.createArm()`.
 *
 * **Memory:** O(nbrArms · arm-state) — per-arm posterior plus a step
 * counter.
 *
 * **Choose:** O(nbrArms) expected; O([maxResamples] · nbrArms) worst case
 * when the second-arm resample loop spins to the cap.
 *
 * **Update:** O(1) on the targeted arm, delegated to `policy.update`.
 *
 * **Randomness:** every posterior sample and the `beta` coin flip use the
 * caller-supplied [random]; reproducible under a fixed seed if the policy
 * is.
 *
 * **Concurrency:** per-arm [com.eignex.kumulant.core.SeriesStat] carries its
 * own concurrency. The step counter is non-atomic — concurrent `choose`
 * calls race on it. Cross-arm snapshot consistency during `choose` is
 * best-effort under racing updates.
 */
class TopTwoThompsonBandit<R : Result>(
    /** Number of arms. */
    override val nbrArms: Int,
    /** Per-arm posterior + arm spec. */
    val policy: ThompsonSampling<R>,
    /** Probability of playing the round's top sample; default 0.5 is Russo's recipe. */
    val beta: Double = 0.5,
    /** Cap on the resample loop when searching for a different second arm. */
    val maxResamples: Int = 32,
    /** Single source of randomness. */
    override val random: Random = Random.Default,
) : UnivariateBandit, PerArmBandit<R> {
    init {
        require(nbrArms >= 2) { "nbrArms must be at least 2 for top-two, got $nbrArms" }
        require(beta in 0.0..1.0) { "beta must lie in [0, 1], got $beta" }
        require(maxResamples > 0) { "maxResamples must be positive, got $maxResamples" }
    }

    private val stats: Array<SeriesStat<R>> = Array(nbrArms) { policy.createArm() }
    private var step: Long = 0L

    /** Choose an arm via the top-two protocol. */
    override fun choose(): Int {
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

    override fun update(armIndex: Int, value: Double, weight: Double) {
        require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
        policy.update(stats[armIndex], value, weight)
    }

    override fun snapshot(): List<R> = stats.map { it.read(0L) }

    override fun armResult(armIndex: Int): R = stats[armIndex].read(0L)

    override fun reset() {
        for (i in 0 until nbrArms) stats[i] = policy.createArm()
        step = 0L
    }

    override fun merge(other: List<R>) {
        require(other.size == nbrArms) {
            "merge: other.size=${other.size} does not match nbrArms=$nbrArms"
        }
        for (i in 0 until nbrArms) stats[i].merge(other[i])
    }

    override fun create(random: Random): TopTwoThompsonBandit<R> =
        TopTwoThompsonBandit(nbrArms, policy, beta, maxResamples, random)
}
