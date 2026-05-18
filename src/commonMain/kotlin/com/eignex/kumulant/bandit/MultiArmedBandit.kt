@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.random.Random

/**
 * Univariate bandit with a fixed number of independent arms. The [policy] owns each
 * arm's accumulator (a kumulant [SeriesStat]); on each [choose] the bandit reads a
 * fresh snapshot per arm and asks the policy to score them.
 *
 * Random source is caller-supplied via [random]. Pass `Random(seed)` for
 * reproducibility, `Random.Default` for shared global state, or any custom
 * implementation (e.g. a thread-local wrapper, or a SecureRandom-backed bridge).
 * The bandit treats [random] as the single source of randomness for every
 * [policy] evaluate call; thread-safety is the caller's responsibility.
 */
class MultiArmedBandit<R : Result>(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Policy that owns the per-arm cumulators and the arm-selection rule. */
    val policy: BanditPolicy<R>,
    override val random: Random = Random.Default,
) : UnivariateBandit<R> {

    init { require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" } }

    private val step = AtomicLong(0L)
    private val arms: Array<SeriesStat<R>> = Array(nbrArms) {
        policy.createArm().also { policy.addArm(it.read(0L)) }
    }

    override fun choose(): Int {
        val t = step.addAndFetch(1L) - 1L
        var bestIdx = 0
        var bestScore = Double.NEGATIVE_INFINITY
        for (i in 0 until nbrArms) {
            val score = policy.evaluate(arms[i].read(0L), t, random)
            if (score > bestScore) {
                bestScore = score
                bestIdx = i
            }
        }
        return bestIdx
    }

    override fun evaluate(armIndex: Int): Double =
        policy.evaluate(arms[armIndex].read(0L), step.load(), random)

    override fun update(armIndex: Int, value: Double, weight: Double) {
        policy.update(arms[armIndex], value, weight)
    }

    override fun snapshot(): List<R> = arms.map { it.read(0L) }

    override fun armResult(armIndex: Int): R = arms[armIndex].read(0L)

    /**
     * Live per-arm accumulator owned by this bandit. Exposed so callers can compose with
     * the stat ecosystem - e.g. inspect the running snapshot, plug into a [com.eignex.kumulant.schema.StatGroup],
     * or apply ops via the live-stat extensions. Writes flow through the policy's
     * [BanditPolicy.update] (use [MultiArmedBandit.update] for that); the returned reference
     * is intended for read-side and composition, not for bypassing the policy.
     */
    fun armStat(armIndex: Int): SeriesStat<R> = arms[armIndex]
}
