@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.Scorable
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.random.Random

/**
 * Univariate bandit with a fixed number of independent arms, each backed by a
 * kumulant [SeriesStat]; on every `choose` the bandit asks the [policy] to
 * score a fresh snapshot per arm and picks the argmax.
 *
 * The selection rule and the arm accumulator both live in [BanditPolicy], so
 * swapping Thompson sampling for UCB1 is a policy swap, not a bandit swap.
 *
 * **Use cases:** stationary multi-armed problems with scalar rewards; any
 * policy expressible as "score each arm independently, pick the max".
 *
 * **Arms:** indexless, `nbrArms` fixed at construction; each arm owns one
 * [SeriesStat] from `policy.createArm()`.
 *
 * **Memory:** O(nbrArms · arm-state); per-arm [SeriesStat] plus a shared
 * atomic step counter.
 *
 * **Choose:** O(nbrArms); one `policy.evaluate` per arm, argmax.
 *
 * **Update:** O(1) on the targeted arm, delegated to `policy.update`.
 *
 * **Randomness:** every `policy.evaluate` and `policy.update` receives the
 * caller-supplied [random]; reproducible under a fixed seed if the policy is.
 *
 * **Concurrency:** per-arm [SeriesStat] carries its own concurrency. The step
 * counter is an atomic so concurrent `choose`s see distinct `t` values;
 * racing `update`s on different arms never block. Cross-arm snapshot
 * consistency is best-effort; a concurrent update may interleave between
 * per-arm reads.
 */
class MultiArmedBandit<R : Result>(
    /** Number of arms in the population. */
    override val nbrArms: Int,
    /** Policy that owns the per-arm cumulators and the arm-selection rule. */
    val policy: BanditPolicy<R>,
    override val random: Random = Random.Default,
) : UnivariateBandit,
    PerArmBandit<R>,
    Scorable {

    init {
        require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
    }

    private val step = AtomicLong(0L)
    private val arms: Array<SeriesStat<R>> = Array(nbrArms) {
        val arm = policy.createArm()
        policy.addArm(arm.read(0L))
        arm
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

    override fun evaluate(armIndex: Int): Double = policy.evaluate(arms[armIndex].read(0L), step.load(), random)

    override fun update(armIndex: Int, value: Double, weight: Double) {
        policy.update(arms[armIndex], value, weight)
    }

    override fun snapshot(): List<R> = arms.map { it.read(0L) }

    override fun armResult(armIndex: Int): R = arms[armIndex].read(0L)

    override fun merge(other: List<R>) {
        require(other.size == nbrArms) {
            "merge: other.size=${other.size} does not match nbrArms=$nbrArms"
        }
        for (i in 0 until nbrArms) {
            val oldSnap = arms[i].read(0L)
            policy.removeArm(oldSnap)
            arms[i].merge(other[i])
            policy.addArm(arms[i].read(0L))
        }
    }

    override fun reset() {
        for (i in 0 until nbrArms) {
            policy.removeArm(arms[i].read(0L))
            arms[i] = policy.createArm()
            policy.addArm(arms[i].read(0L))
        }
        step.store(0L)
    }

    override fun create(random: Random): MultiArmedBandit<R> = MultiArmedBandit(nbrArms, policy, random)

    /**
     * Live per-arm accumulator owned by this bandit. Exposed so callers can compose with
     * the stat ecosystem - e.g. inspect the running snapshot, plug into a
     * [com.eignex.kumulant.schema.runtime.StatGroup], or apply ops via the live-stat
     * extensions. Writes flow through the policy's
     * [BanditPolicy.update] (use [MultiArmedBandit.update] for that); the returned reference
     * is intended for read-side and composition, not for bypassing the policy.
     */
    fun armStat(armIndex: Int): SeriesStat<R> = arms[armIndex]
}
