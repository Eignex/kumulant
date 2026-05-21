package com.eignex.kumulant.bench

import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.math.DenseVector
import kotlin.random.Random

/**
 * Accuracy report for every [BanditSpec]: drives the bandit for `rounds` rounds
 * against the spec's own oracle, tracks cumulative regret against the always-
 * optimal-arm policy, and reports the fraction of rounds where the chosen arm
 * matched the oracle-optimal arm in the last 10% of rounds.
 *
 * Regret is the expected reward of the optimal arm minus the realized reward
 * of the chosen arm, summed across rounds. Lower is better.
 *
 * Driven by these system properties (all optional):
 *  - `bench.rounds`     rounds per spec, default 5000
 *  - `bench.banditSeed` PRNG seed for the bandit, default 0xB411D17
 *  - `bench.oracleSeed` PRNG seed for the oracle, default 0xC0FFEE
 *
 * Invoke via `./gradlew :kumulant-bench:analyzeBanditAccuracy`.
 */
fun main() {
    val rounds = System.getProperty("bench.rounds")?.toInt() ?: 5000
    val banditSeed = System.getProperty("bench.banditSeed")?.toLong() ?: 0xB411D17L
    val oracleSeed = System.getProperty("bench.oracleSeed")?.toLong() ?: 0xC0FFEEL

    println("Bandit accuracy report. $rounds rounds per spec.")
    println(
        "%-32s  %12s  %14s  %16s  %14s".format(
            "spec", "rounds", "total reward", "cumulative regret", "tail optimal%",
        ),
    )
    println("-".repeat(96))

    for (spec in allBanditSpecs) {
        val report = when (spec) {
            is UnivariateBanditSpec -> measureUnivariate(spec, rounds, banditSeed, oracleSeed)
            is ContextualBanditSpec -> measureContextual(spec, rounds, banditSeed, oracleSeed)
        }
        println(
            "%-32s  %12d  %14.4f  %16.4f  %13.1f%%".format(
                spec.name, rounds, report.totalReward, report.cumulativeRegret, report.tailOptimalPct,
            ),
        )
    }
}

private data class AccuracyReport(
    val totalReward: Double,
    val cumulativeRegret: Double,
    val tailOptimalPct: Double,
)

private fun measureUnivariate(
    spec: UnivariateBanditSpec,
    rounds: Int,
    banditSeed: Long,
    oracleSeed: Long,
): AccuracyReport {
    val bandit: UnivariateBandit = spec.build(Random(banditSeed))
    val oracleRng = Random(oracleSeed)
    val rewardRng = Random(oracleSeed xor 0x55_AA_55_AAL) // independent stream for the regret reference
    val tailStart = rounds * 9 / 10
    var totalReward = 0.0
    var regret = 0.0
    var tailOptimal = 0
    for (t in 0 until rounds) {
        val chosen = bandit.choose()
        val reward = spec.sampleReward(chosen, oracleRng)
        bandit.update(chosen, reward)
        val optimalReward = spec.sampleReward(spec.optimalArm, rewardRng)
        totalReward += reward
        regret += optimalReward - reward
        if (t >= tailStart && chosen == spec.optimalArm) tailOptimal++
    }
    val tailRounds = rounds - tailStart
    return AccuracyReport(
        totalReward = totalReward,
        cumulativeRegret = regret,
        tailOptimalPct = if (tailRounds == 0) 0.0 else 100.0 * tailOptimal / tailRounds,
    )
}

private fun measureContextual(
    spec: ContextualBanditSpec,
    rounds: Int,
    banditSeed: Long,
    oracleSeed: Long,
): AccuracyReport {
    val bandit: ContextualBandit = spec.build(Random(banditSeed))
    val oracleRng = Random(oracleSeed)
    val rewardRng = Random(oracleSeed xor 0x55_AA_55_AAL)
    val tailStart = rounds * 9 / 10
    var totalReward = 0.0
    var regret = 0.0
    var tailOptimal = 0
    for (t in 0 until rounds) {
        val ctx = spec.sampleContext(oracleRng)
        val x = DenseVector.of(ctx)
        val chosen = bandit.choose(x)
        val reward = spec.sampleReward(chosen, ctx, oracleRng)
        bandit.update(chosen, x, reward)
        val opt = spec.optimalArm(ctx)
        val optimalReward = spec.sampleReward(opt, ctx, rewardRng)
        totalReward += reward
        regret += optimalReward - reward
        if (t >= tailStart && chosen == opt) tailOptimal++
    }
    val tailRounds = rounds - tailStart
    return AccuracyReport(
        totalReward = totalReward,
        cumulativeRegret = regret,
        tailOptimalPct = if (tailRounds == 0) 0.0 else 100.0 * tailOptimal / tailRounds,
    )
}
