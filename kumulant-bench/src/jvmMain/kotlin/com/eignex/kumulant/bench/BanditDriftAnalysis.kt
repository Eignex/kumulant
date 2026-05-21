package com.eignex.kumulant.bench

import com.eignex.kumulant.bandit.Bandit
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.abs
import kotlin.random.Random

/**
 * Concurrency-drift report for every [BanditSpec]: drives the bandit with N
 * threads sharing the same instance, totals the reward observed across all
 * rounds, and compares against a serial reference run with the same total
 * round count.
 *
 * The reported drift is `|concurrentTotalReward - serialTotalReward|`. The
 * intent is "does the bandit stay behaviourally consistent under contention",
 * not bit-for-bit equality — the choose path is randomised and order-sensitive.
 *
 * Driven by these system properties (all optional):
 *  - `bench.threads`        worker thread count, default 4
 *  - `bench.roundsPerThread` rounds each worker performs, default 2500
 *  - `bench.banditSeed`     PRNG seed for the bandit, default 0xB411D17
 *  - `bench.oracleSeed`     PRNG seed for the oracle, default 0xC0FFEE
 *
 * Invoke via `./gradlew :kumulant-bench:analyzeBanditDrift`.
 */
fun main() {
    val threads = System.getProperty("bench.threads")?.toInt() ?: 4
    val roundsPerThread = System.getProperty("bench.roundsPerThread")?.toInt() ?: 2500
    val banditSeed = System.getProperty("bench.banditSeed")?.toLong() ?: 0xB411D17L
    val oracleSeed = System.getProperty("bench.oracleSeed")?.toLong() ?: 0xC0FFEEL
    val totalRounds = threads * roundsPerThread

    println(
        "Bandit drift report. $threads threads × $roundsPerThread rounds each ($totalRounds total).",
    )
    println(
        "%-32s  %18s  %18s  %14s  %14s".format(
            "spec", "serial reward", "concurrent reward", "abs drift", "rel drift",
        ),
    )
    println("-".repeat(106))

    for (spec in allBanditSpecs) {
        val serial = driveSerial(spec, totalRounds, banditSeed, oracleSeed)
        val concurrent = driveConcurrent(spec, threads, roundsPerThread, banditSeed, oracleSeed)
        val absDrift = abs(serial - concurrent)
        val relDrift = if (serial != 0.0) absDrift / abs(serial) else absDrift
        println(
            "%-32s  %18.4f  %18.4f  %14.4f  %14.6f".format(
                spec.name, serial, concurrent, absDrift, relDrift,
            ),
        )
    }
}

private fun <B : Bandit> driveSerial(
    spec: BanditSpec<B>,
    rounds: Int,
    banditSeed: Long,
    oracleSeed: Long,
): Double {
    val bandit = spec.build(Random(banditSeed))
    val oracleRng = Random(oracleSeed)
    val refRng = Random(oracleSeed xor 0x55_AA_55_AAL)
    var total = 0.0
    repeat(rounds) {
        total += spec.regretCycle(bandit, oracleRng, refRng).reward
    }
    return total
}

private fun <B : Bandit> driveConcurrent(
    spec: BanditSpec<B>,
    threads: Int,
    roundsPerThread: Int,
    banditSeed: Long,
    oracleSeed: Long,
): Double {
    val bandit = spec.build(Random(banditSeed))
    val executor = Executors.newFixedThreadPool(threads)
    val start = CountDownLatch(1)
    val done = CountDownLatch(threads)
    val perThreadTotals = DoubleArray(threads)
    repeat(threads) { tid ->
        executor.submit {
            // Each worker uses its own oracle stream so reward sampling doesn't race.
            val oracleRng = Random(oracleSeed xor (tid.toLong() * 0x9E37_79B9L))
            val refRng = Random((oracleSeed xor 0x55_AA_55_AAL) xor (tid.toLong() * 0x9E37_79B9L))
            var local = 0.0
            start.await()
            repeat(roundsPerThread) {
                local += spec.regretCycle(bandit, oracleRng, refRng).reward
            }
            perThreadTotals[tid] = local
            done.countDown()
        }
    }
    start.countDown()
    check(done.await(60, TimeUnit.SECONDS)) { "${spec.name}: timed out" }
    executor.shutdownNow()
    return perThreadTotals.sum()
}
