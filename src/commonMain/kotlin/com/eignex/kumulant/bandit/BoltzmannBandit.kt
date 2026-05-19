package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.math.exp
import kotlin.math.pow
import kotlin.random.Random

/**
 * Boltzmann exploration (a.k.a. softmax bandit): pick arm `a` with probability
 * proportional to `exp(mean[a] / tau(t))`, where `tau(t)` is the temperature at round
 * `t`. High temperature flattens the distribution toward uniform exploration; low
 * temperature sharpens toward greedy exploitation.
 *
 * Default schedule cools as `tau(t) = max(minTau, initialTau / t^decay)` — Cesa-Bianchi
 * & Fischer's classical recipe with `decay = 1`. Pass a constant schedule (decay = 0)
 * for fixed-temperature softmax.
 *
 * Standalone bandit because the play distribution is a softmax over all arms, not an
 * argmax over independent per-arm scores.
 */
class BoltzmannBandit(
    /** Number of arms. */
    val nbrArms: Int,
    /** Per-arm prior on the running reward mean. */
    priorMean: Double = 0.0,
    /** Per-arm prior pseudo-count. */
    priorWeight: Double = 0.02,
    /** Initial temperature; the schedule cools from here. */
    val initialTau: Double = 1.0,
    /** Floor on the temperature so the softmax never collapses to a delta. */
    val minTau: Double = 1e-3,
    /** Cooling decay exponent: `tau(t) = initialTau / t^decay`. `0.0` is fixed-temperature. */
    val decay: Double = 1.0,
    /** Single source of randomness. */
    val random: Random = Random.Default,
) {
    init {
        require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
        require(initialTau > 0.0) { "initialTau must be positive, got $initialTau" }
        require(minTau > 0.0) { "minTau must be positive, got $minTau" }
        require(decay >= 0.0) { "decay must be non-negative, got $decay" }
    }

    private val armSpec = MeanArm(priorMean, priorWeight)
    private val stats: Array<SeriesStat<WeightedMeanResult>> = Array(nbrArms) { armSpec.createStat() }
    private var step: Long = 0L

    /** Sample arm from the softmax of per-arm means at the current temperature. */
    fun choose(): Int {
        val p = playDistribution()
        var u = random.nextDouble()
        for (a in 0 until nbrArms) {
            u -= p[a]
            if (u <= 0.0) return a
        }
        return nbrArms - 1
    }

    /** Current play distribution: `softmax(mean / tau(t))`. Also advances the internal step. */
    fun playDistribution(): DoubleArray {
        step++
        val tau = temperature()
        val means = DoubleArray(nbrArms) { stats[it].read(0L).mean }
        // Stable softmax: subtract the max before exp.
        var maxM = means[0]
        for (m in means) if (m > maxM) maxM = m
        val out = DoubleArray(nbrArms)
        var sum = 0.0
        for (a in 0 until nbrArms) {
            val e = exp((means[a] - maxM) / tau)
            out[a] = e
            sum += e
        }
        for (a in 0 until nbrArms) out[a] /= sum
        return out
    }

    /** Fold `(arm, reward)` into the per-arm mean. */
    fun update(armIndex: Int, value: Double, weight: Double = 1.0) {
        require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
        stats[armIndex].update(value, 0L, weight)
    }

    /** Live per-arm running-mean snapshot at [armIndex]. */
    fun armResult(armIndex: Int): WeightedMeanResult = stats[armIndex].read(0L)

    /** Snapshot every arm's mean. */
    fun snapshot(): List<WeightedMeanResult> = stats.map { it.read(0L) }

    /** Current temperature: `max(minTau, initialTau / step^decay)`. */
    fun temperature(): Double {
        if (decay == 0.0) return initialTau
        val s = step.coerceAtLeast(1L).toDouble()
        return maxOf(minTau, initialTau / s.pow(decay))
    }

    /** Reset arms to priors and step counter to zero. */
    fun reset() {
        for (i in 0 until nbrArms) stats[i] = armSpec.createStat()
        step = 0L
    }

    /** Spawn a fresh bandit with the same configuration. */
    fun create(random: Random = this.random): BoltzmannBandit =
        BoltzmannBandit(nbrArms, armSpec.priorMean, armSpec.priorWeight, initialTau, minTau, decay, random)
}
