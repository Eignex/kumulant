package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import kotlin.random.Random

/**
 * Top-Two Thompson Sampling (Russo 2020); pure-exploration variant of
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
 * Arm selection uses joint sampling and conditional resampling. Per-arm posterior
 * state is available through [PerArmBandit].
 *
 * **Use cases:** best-arm identification and pure-exploration problems where
 * shrinking the regret gap matters more than minimising cumulative regret;
 * any posterior expressible as a [ThompsonSampling] policy.
 *
 * **Arms:** indexless, `nbrArms ≥ 2` fixed at construction; each arm owns
 * one posterior cell from `policy.createArm()`.
 *
 * **Memory:** O(nbrArms · arm-state); one inner [MultiArmedBandit] over [policy].
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
 * **Concurrency:** per-arm [com.eignex.kumulant.core.SeriesStat]s are built
 * with [concurrency], as in [MultiArmedBandit]. Cross-arm snapshot consistency
 * during `choose` is best-effort under racing updates.
 */
class TopTwoThompsonBandit<R : Result>(
    override val nbrArms: Int,
    /** Per-arm posterior + arm spec. */
    val policy: ThompsonSampling<R>,
    /** Probability of playing the round's top sample; default 0.5 is Russo's recipe. */
    val beta: Double = 0.5,
    /** Cap on the resample loop when searching for a different second arm. */
    val maxResamples: Int = 32,
    override val random: Random = Random.Default,
    /** Thread-safety mode of every per-arm accumulator. */
    val concurrency: Concurrency = Concurrency.None,
) : UnivariateBandit,
    PerArmBandit<R> {
    init {
        require(nbrArms >= 2) { "nbrArms must be at least 2 for top-two, got $nbrArms" }
        require(beta in 0.0..1.0) { "beta must lie in [0, 1], got $beta" }
        require(maxResamples > 0) { "maxResamples must be positive, got $maxResamples" }
    }

    // Arm storage, updates and snapshots are a multi-armed Thompson bandit's; sharing its random
    // keeps the draw sequence of a top-two round identical to sampling each arm here.
    private val bandit = MultiArmedBandit(nbrArms, policy, random, concurrency)

    override fun choose(): Int {
        val arm1 = bandit.choose()
        if (random.nextDouble() < beta) return arm1
        repeat(maxResamples) {
            val candidate = bandit.choose()
            if (candidate != arm1) return candidate
        }
        return arm1
    }

    override fun update(armIndex: Int, value: Double, weight: Double) = bandit.update(armIndex, value, weight)

    override fun snapshot(): List<R> = bandit.snapshot()

    override fun armResult(armIndex: Int): R = bandit.armResult(armIndex)

    override fun reset() = bandit.reset()

    override fun merge(other: List<R>) = bandit.merge(other)

    override fun create(random: Random): TopTwoThompsonBandit<R> =
        TopTwoThompsonBandit(nbrArms, policy, beta, maxResamples, random, concurrency)
}
