package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.bandit.requireArmIndex
import com.eignex.kumulant.bandit.requireMergeSize
import com.eignex.kumulant.bandit.requireNbrArms
import com.eignex.kumulant.bandit.sampleFromDistribution
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.softmaxInPlace
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.math.pow
import kotlin.random.Random

/**
 * Boltzmann exploration (a.k.a. softmax bandit): samples arm `a` with
 * probability proportional to `exp(mean(a) / tau(t))`, where `tau(t)` is the
 * temperature at round `t` and per-arm means are tracked by independent
 * [com.eignex.kumulant.stat.summary.MeanStat] cells.
 *
 * Default schedule cools as `tau(t) = max(minTau, initialTau / t^decay)`;
 * Cesa-Bianchi & Fischer's classical recipe with `decay = 1`. Pass a constant
 * schedule (`decay = 0`) for fixed-temperature softmax. High temperature
 * flattens the distribution toward uniform exploration; low temperature
 * sharpens toward greedy exploitation.
 *
 * Arm selection uses a joint softmax distribution. Per-arm `(mean, totalWeights)`
 * state is available through [PerArmBandit].
 *
 * **Use cases:** stationary scalar-reward problems where smooth probabilistic
 * exploration is preferable to UCB's deterministic confidence bounds; any
 * setting where a tunable cooling schedule is convenient.
 *
 * **Arms:** indexless, `nbrArms` fixed at construction; each arm owns one
 * [com.eignex.kumulant.stat.summary.MeanStat].
 *
 * **Memory:** O(nbrArms); one mean cell per arm plus a step counter.
 *
 * **Choose:** O(nbrArms); softmax over per-arm means, inverse-CDF sample.
 *
 * **Update:** O(1) on the targeted arm.
 *
 * **Randomness:** every `choose` consumes one `random.nextDouble()` for the
 * softmax draw; reproducible under a fixed seed.
 *
 * **Concurrency:** not thread-safe. Per-arm stats and the cooling step counter
 * are unsynchronised; serialise access externally for multi-thread use.
 */
class BoltzmannBandit(
    override val nbrArms: Int,
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
    override val random: Random = Random.Default,
) : UnivariateBandit,
    PerArmBandit<WeightedMeanResult> {
    init {
        requireNbrArms(nbrArms)
        require(initialTau > 0.0) { "initialTau must be positive, got $initialTau" }
        require(minTau > 0.0) { "minTau must be positive, got $minTau" }
        require(decay >= 0.0) { "decay must be non-negative, got $decay" }
    }

    private val armSpec = MeanArm(priorMean, priorWeight)
    private val stats: Array<SeriesStat<WeightedMeanResult>> = Array(nbrArms) { armSpec.createStat() }
    private var step: Long = 0L

    override fun choose(): Int {
        val p = playDistribution()
        return random.sampleFromDistribution(p)
    }

    /** Current play distribution: `softmax(mean / tau(t))`. Also advances the internal step. */
    fun playDistribution(): DoubleArray {
        step++
        val tau = temperature()
        // Scale directly into the logits array to avoid allocating a second array of means.
        val logits = DoubleArray(nbrArms) { stats[it].read(0L).mean / tau }
        // Non-finite logits cannot form a usable distribution; uniform sampling keeps every arm reachable.
        if (logits.any { !it.isFinite() }) return DoubleArray(nbrArms) { 1.0 / nbrArms }
        logits.softmaxInPlace()
        return logits
    }

    override fun update(armIndex: Int, value: Double, weight: Double) {
        requireArmIndex(armIndex, nbrArms)
        stats[armIndex].update(value, 0L, weight)
    }

    override fun armResult(armIndex: Int): WeightedMeanResult = stats[armIndex].read(0L)

    override fun snapshot(): List<WeightedMeanResult> = stats.map { it.read(0L) }

    override fun merge(other: List<WeightedMeanResult>) {
        requireMergeSize(other.size, nbrArms)
        for (i in 0 until nbrArms) stats[i].merge(other[i])
    }

    /** Current temperature: `max(minTau, initialTau / step^decay)`. */
    fun temperature(): Double {
        if (decay == 0.0) return initialTau
        val s = step.coerceAtLeast(1L).toDouble()
        return maxOf(minTau, initialTau / s.pow(decay))
    }

    override fun reset() {
        for (i in 0 until nbrArms) stats[i] = armSpec.createStat()
        step = 0L
    }

    override fun create(random: Random): BoltzmannBandit =
        BoltzmannBandit(nbrArms, armSpec.priorMean, armSpec.priorWeight, initialTau, minTau, decay, random)
}
