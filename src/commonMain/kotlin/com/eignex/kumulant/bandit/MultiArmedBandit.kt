@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.random.Random

/**
 * Thread-safe deterministic Random sequence: every [next] call advances a CAS-tracked
 * uniquifier so concurrent callers get distinct, reproducible RNGs. The single-threaded
 * sequence is fully determined by [sequenceStart], so unit tests stay reproducible.
 */
internal class RandomSequence(val sequenceStart: Int) {
    private val seedUniquifier = AtomicInt(sequenceStart)
    fun next(): Random {
        while (true) {
            val current = seedUniquifier.load()
            val next = (1 + current) * 741103587 // linear congruent generator constant
            if (seedUniquifier.compareAndSet(current, next)) return Random(next)
        }
    }
}

/**
 * Univariate bandit with a fixed number of independent arms. The [policy] owns each
 * arm's accumulator (a kumulant [SeriesStat]); on each [choose] the bandit reads a fresh
 * snapshot per arm and asks the policy to score them.
 */
class MultiArmedBandit<R : Result>(
    val nbrArms: Int,
    val policy: BanditPolicy<R>,
    override val randomSeed: Int = Random.Default.nextInt(),
    override val maximize: Boolean = true,
) : UnivariateBandit<R> {

    init { require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" } }

    private val randomSequence = RandomSequence(randomSeed)
    private val step = AtomicLong(0L)
    private val arms: Array<SeriesStat<R>> = Array(nbrArms) {
        policy.createArm().also { policy.addArm(it.read(0L)) }
    }

    override fun choose(): Int {
        val t = step.addAndFetch(1L) - 1L
        val rng = randomSequence.next()
        var bestIdx = 0
        var bestScore = Double.NEGATIVE_INFINITY
        for (i in 0 until nbrArms) {
            val score = policy.evaluate(arms[i].read(0L), t, maximize, rng)
            if (score > bestScore) { bestScore = score; bestIdx = i }
        }
        return bestIdx
    }

    override fun update(armIndex: Int, value: Double, weight: Double) {
        policy.update(arms[armIndex], value, weight)
    }

    override fun snapshot(): List<R> = arms.map { it.read(0L) }
}
