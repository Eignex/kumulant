package com.eignex.kumulant.bench

import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.math.DenseVector
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
 * The reported drift is `|concurrentTotalReward - serialTotalReward|`, plus the
 * absolute difference in tail-optimal selection rate. The intent is "does the
 * bandit stay behaviourally consistent under contention", not bit-for-bit
 * equality (the choose path is randomised and order-sensitive).
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

private fun driveSerial(spec: BanditSpec, rounds: Int, banditSeed: Long, oracleSeed: Long): Double {
    val driver = spec.newDriver(Random(banditSeed), Random(oracleSeed))
    var total = 0.0
    when (spec) {
        is UnivariateBanditSpec -> {
            val bandit = driver.bandit as UnivariateBandit
            val oracleRng = Random(oracleSeed)
            repeat(rounds) {
                val i = bandit.choose()
                val r = spec.sampleReward(i, oracleRng)
                bandit.update(i, r)
                total += r
            }
        }
        is ContextualBanditSpec -> {
            val bandit = driver.bandit as ContextualBandit
            val oracleRng = Random(oracleSeed)
            repeat(rounds) {
                val ctx = spec.sampleContext(oracleRng)
                val x = DenseVector.of(ctx)
                val i = bandit.choose(x)
                val r = spec.sampleReward(i, ctx, oracleRng)
                bandit.update(i, x, r)
                total += r
            }
        }
    }
    return total
}

private fun driveConcurrent(
    spec: BanditSpec,
    threads: Int,
    roundsPerThread: Int,
    banditSeed: Long,
    oracleSeed: Long,
): Double {
    val driver = spec.newDriver(Random(banditSeed), Random(oracleSeed))
    val executor = Executors.newFixedThreadPool(threads)
    val start = CountDownLatch(1)
    val done = CountDownLatch(threads)
    val perThreadTotals = DoubleArray(threads)
    repeat(threads) { tid ->
        executor.submit {
            // Each worker uses its own oracle stream so reward sampling doesn't race.
            val oracleRng = Random(oracleSeed xor (tid.toLong() * 0x9E37_79B9L))
            var local = 0.0
            start.await()
            when (spec) {
                is UnivariateBanditSpec -> {
                    val bandit = driver.bandit as UnivariateBandit
                    repeat(roundsPerThread) {
                        val i = bandit.choose()
                        val r = spec.sampleReward(i, oracleRng)
                        bandit.update(i, r)
                        local += r
                    }
                }
                is ContextualBanditSpec -> {
                    val bandit = driver.bandit as ContextualBandit
                    repeat(roundsPerThread) {
                        val ctx = spec.sampleContext(oracleRng)
                        val x = DenseVector.of(ctx)
                        val i = bandit.choose(x)
                        val r = spec.sampleReward(i, ctx, oracleRng)
                        bandit.update(i, x, r)
                        local += r
                    }
                }
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
