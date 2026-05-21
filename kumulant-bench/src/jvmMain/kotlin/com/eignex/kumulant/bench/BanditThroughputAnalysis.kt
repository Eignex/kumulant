package com.eignex.kumulant.bench

import java.lang.management.ManagementFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

/**
 * Throughput report for every [BanditSpec]: measures wall-clock (choose, play,
 * update) cycles per second at 1 and N threads, and reports ns/cycle, cycles/sec,
 * GC time spent during the cell, and the thread-scaling factor.
 *
 * Driven by the same system properties as [main] in [ThroughputAnalysis.kt]:
 *  - `bench.threads`        multi-threaded cell width, default 4
 *  - `bench.warmupMillis`   per-cell warmup, default 200
 *  - `bench.measureMillis`  per-cell measurement window, default 500
 *  - `bench.banditSeed`     PRNG seed for the bandit, default 0xB411D17
 *  - `bench.oracleSeed`     PRNG seed for the reward oracle, default 0xC0FFEE
 *
 * Invoke via `./gradlew :kumulant-bench:analyzeBanditThroughput`.
 */
fun main() {
    val threads = System.getProperty("bench.threads")?.toInt() ?: 4
    val warmupMs = System.getProperty("bench.warmupMillis")?.toLong() ?: 200L
    val measureMs = System.getProperty("bench.measureMillis")?.toLong() ?: 500L
    val banditSeed = System.getProperty("bench.banditSeed")?.toLong() ?: 0xB411D17L
    val oracleSeed = System.getProperty("bench.oracleSeed")?.toLong() ?: 0xC0FFEEL

    println(
        "Bandit throughput report. warmup ${warmupMs}ms, measure ${measureMs}ms per cell, $threads worker threads",
    )
    println(
        "%-32s  %7s  %14s  %14s  %10s  %9s".format(
            "spec", "threads", "cycles/sec", "ns/cycle", "gc ms", "scale",
        ),
    )
    println("-".repeat(100))

    for (spec in allBanditSpecs) {
        val single = measureCell(spec, threads = 1, warmupMs, measureMs, banditSeed, oracleSeed)
        printRow(spec.name, 1, single, baseline = single)
        val multi = measureCell(spec, threads = threads, warmupMs, measureMs, banditSeed, oracleSeed)
        printRow(spec.name, threads, multi, baseline = single)
        println()
    }
}

private data class BanditCell(val cycles: Long, val elapsedNanos: Long, val gcMillis: Long) {
    val cyclesPerSec: Double get() = cycles.toDouble() * 1e9 / elapsedNanos.toDouble()
    val nanosPerCycle: Double get() = elapsedNanos.toDouble() / cycles.toDouble()
}

private fun printRow(name: String, threads: Int, c: BanditCell, baseline: BanditCell) {
    val scale = c.cyclesPerSec / baseline.cyclesPerSec
    println(
        "%-32s  %7d  %14s  %14.2f  %10d  %9.2fx".format(
            name, threads, formatRate(c.cyclesPerSec), c.nanosPerCycle, c.gcMillis, scale,
        ),
    )
}

private fun formatRate(opsPerSec: Double): String = when {
    opsPerSec >= 1e9 -> "%.2f G".format(opsPerSec / 1e9)
    opsPerSec >= 1e6 -> "%.2f M".format(opsPerSec / 1e6)
    opsPerSec >= 1e3 -> "%.2f K".format(opsPerSec / 1e3)
    else -> "%.0f".format(opsPerSec)
}

private fun measureCell(
    spec: BanditSpec,
    threads: Int,
    warmupMs: Long,
    measureMs: Long,
    banditSeed: Long,
    oracleSeed: Long,
): BanditCell {
    val driver = spec.newDriver(Random(banditSeed), Random(oracleSeed))
    runWindow(driver, threads, warmupMs)
    System.gc()
    val gcBefore = gcMillis()
    val ops = runWindow(driver, threads, measureMs)
    val gcAfter = gcMillis()
    return BanditCell(cycles = ops.cyclesTotal, elapsedNanos = ops.elapsedNanos, gcMillis = gcAfter - gcBefore)
}

private data class BanditRunResult(val cyclesTotal: Long, val elapsedNanos: Long)

private fun runWindow(driver: BanditDriver, threads: Int, durationMs: Long): BanditRunResult {
    if (threads == 1) {
        val deadline = System.nanoTime() + durationMs * 1_000_000L
        var cycles = 0L
        val t0 = System.nanoTime()
        while (System.nanoTime() < deadline) {
            driver.cycle()
            cycles++
        }
        return BanditRunResult(cycles, System.nanoTime() - t0)
    }
    val executor = Executors.newFixedThreadPool(threads)
    val stop = AtomicBoolean(false)
    val cyclesTotal = AtomicLong(0)
    val start = CountDownLatch(1)
    val done = CountDownLatch(threads)
    repeat(threads) {
        executor.submit {
            start.await()
            var local = 0L
            while (!stop.get()) {
                driver.cycle()
                local++
            }
            cyclesTotal.addAndGet(local)
            done.countDown()
        }
    }
    val t0 = System.nanoTime()
    start.countDown()
    Thread.sleep(durationMs)
    stop.set(true)
    check(done.await(30, TimeUnit.SECONDS)) { "timed out waiting for bandit driver to finish" }
    val elapsed = System.nanoTime() - t0
    executor.shutdownNow()
    return BanditRunResult(cyclesTotal.get(), elapsed)
}

private fun gcMillis(): Long =
    ManagementFactory.getGarbageCollectorMXBeans().sumOf { it.collectionTime.coerceAtLeast(0L) }
