package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.math.exp
import kotlin.math.pow
import kotlin.random.Random

/**
 * Boltzmann exploration (a.k.a. softmax bandit): samples arm `a` with
 * probability proportional to `exp(mean[a] / tau(t))`, where `tau(t)` is the
 * temperature at round `t` and per-arm means are tracked by independent
 * [com.eignex.kumulant.stat.summary.MeanStat] cells.
 *
 * Default schedule cools as `tau(t) = max(minTau, initialTau / t^decay)` —
 * Cesa-Bianchi & Fischer's classical recipe with `decay = 1`. Pass a constant
 * schedule (`decay = 0`) for fixed-temperature softmax. High temperature
 * flattens the distribution toward uniform exploration; low temperature
 * sharpens toward greedy exploitation.
 *
 * The play distribution is a softmax over all arms, not an argmax over
 * independent per-arm scores — so this bandit doesn't expose
 * [com.eignex.kumulant.bandit.Scorable], but its per-arm
 * `(mean, totalWeights)` state still fits [PerArmBandit].
 *
 * **Use cases:** stationary scalar-reward problems where smooth probabilistic
 * exploration is preferable to UCB's deterministic confidence bounds; any
 * setting where a tunable cooling schedule is convenient.
 *
 * **Arms:** indexless, `nbrArms` fixed at construction; each arm owns one
 * [com.eignex.kumulant.stat.summary.MeanStat].
 *
 * **Memory:** O(nbrArms) — one mean cell per arm plus a step counter.
 *
 * **Choose:** O(nbrArms) — softmax over per-arm means, inverse-CDF sample.
 *
 * **Update:** O(1) on the targeted arm.
 *
 * **Randomness:** every `choose` consumes one `random.nextDouble()` for the
 * softmax draw; reproducible under a fixed seed.
 *
 * **Concurrency:** per-arm [com.eignex.kumulant.core.SeriesStat] carries its
 * own concurrency. The step counter is non-atomic — concurrent `choose` calls
 * race on it and may yield duplicate `t` values; pin to a single thread when
 * the cooling schedule must be exact.
 */
class BoltzmannBandit(
    /** Number of arms. */
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
    /** Single source of randomness. */
    override val random: Random = Random.Default,
) : UnivariateBandit, PerArmBandit<WeightedMeanResult> {
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
    override fun choose(): Int {
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
    override fun update(armIndex: Int, value: Double, weight: Double) {
        require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
        stats[armIndex].update(value, 0L, weight)
    }

    override fun armResult(armIndex: Int): WeightedMeanResult = stats[armIndex].read(0L)

    override fun snapshot(): List<WeightedMeanResult> = stats.map { it.read(0L) }

    override fun merge(other: List<WeightedMeanResult>) {
        require(other.size == nbrArms) {
            "merge: other.size=${other.size} does not match nbrArms=$nbrArms"
        }
        for (i in 0 until nbrArms) stats[i].merge(other[i])
    }

    /** Current temperature: `max(minTau, initialTau / step^decay)`. */
    fun temperature(): Double {
        if (decay == 0.0) return initialTau
        val s = step.coerceAtLeast(1L).toDouble()
        return maxOf(minTau, initialTau / s.pow(decay))
    }

    /** Reset arms to priors and step counter to zero. */
    override fun reset() {
        for (i in 0 until nbrArms) stats[i] = armSpec.createStat()
        step = 0L
    }

    /** Spawn a fresh bandit with the same configuration. */
    override fun create(random: Random): BoltzmannBandit =
        BoltzmannBandit(nbrArms, armSpec.priorMean, armSpec.priorWeight, initialTau, minTau, decay, random)
}
