package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import jdk.jfr.Configuration
import jdk.jfr.Recording

/**
 * Throughput report: for every [StatSpec], measures wall-clock update throughput
 * under each [Concurrency] level at 1 and N threads, and reports ns/op, ops/sec,
 * GC time spent during the cell, and the thread-scaling factor.
 *
 * Optionally wraps the run in a JFR recording so the cells map onto a flight
 * recording you can open in JMC for CPU/allocation profiling.
 *
 * Driven by these system properties (all optional):
 *  - `bench.threads`            multi-threaded cell width, default 4
 *  - `bench.warmupMillis`       per-cell warmup, default 200
 *  - `bench.measureMillis`      per-cell measurement window, default 500
 *  - `bench.jfr`                "true" to record, default false
 *  - `bench.jfr.path`           output path, default build/reports/bench/throughput.jfr
 *  - `bench.jfr.profile`        JFR config: "default" (low overhead) or "profile" (~2%)
 *
 * Invoke via `./gradlew :kumulant-bench:analyzeThroughput`.
 */
fun main() {
    val threads = System.getProperty("bench.threads")?.toInt() ?: 4
    val warmupMs = System.getProperty("bench.warmupMillis")?.toLong() ?: 200L
    val measureMs = System.getProperty("bench.measureMillis")?.toLong() ?: 500L
    val jfrEnabled = System.getProperty("bench.jfr")?.toBoolean() ?: false
    val jfrPath = Paths.get(
        System.getProperty("bench.jfr.path") ?: "build/reports/bench/throughput.jfr",
    )
    val jfrProfile = System.getProperty("bench.jfr.profile") ?: "default"

    val recording = if (jfrEnabled) startRecording(jfrPath, jfrProfile) else null

    println(
        "Throughput report — warmup ${warmupMs}ms, measure ${measureMs}ms per cell, $threads worker threads",
    )
    if (recording != null) println("JFR recording: ${jfrPath.toAbsolutePath()} (profile=$jfrProfile)")
    println(
        "%-32s  %-10s  %7s  %14s  %14s  %10s  %9s".format(
            "stat", "level", "threads", "ops/sec", "ns/op", "gc ms", "scale",
        ),
    )
    println("-".repeat(110))

    for (spec in allSpecs) {
        val serial = measureCell(spec, Concurrency.None, threads = 1, warmupMs, measureMs)
        printRow(spec.name, Concurrency.None, 1, serial, baseline = serial)
        for (level in listOf(Concurrency.Relaxed, Concurrency.Strict, Concurrency.HighWrite)) {
            val single = measureCell(spec, level, threads = 1, warmupMs, measureMs)
            printRow(spec.name, level, 1, single, baseline = serial)
            val multi = measureCell(spec, level, threads = threads, warmupMs, measureMs)
            printRow(spec.name, level, threads, multi, baseline = single)
        }
        println()
    }

    recording?.let {
        it.stop()
        it.close()
        println("JFR recording written to ${jfrPath.toAbsolutePath()}")
    }
}

private data class Cell(val ops: Long, val elapsedNanos: Long, val gcMillis: Long) {
    val opsPerSec: Double get() = ops.toDouble() * 1e9 / elapsedNanos.toDouble()
    val nanosPerOp: Double get() = elapsedNanos.toDouble() / ops.toDouble()
}

private fun printRow(name: String, level: Concurrency, threads: Int, c: Cell, baseline: Cell) {
    val scale = c.opsPerSec / baseline.opsPerSec
    println(
        "%-32s  %-10s  %7d  %14s  %14.2f  %10d  %9.2fx".format(
            name, level.name, threads, formatRate(c.opsPerSec), c.nanosPerOp, c.gcMillis, scale,
        ),
    )
}

private fun formatRate(opsPerSec: Double): String = when {
    opsPerSec >= 1e9 -> "%.2f G".format(opsPerSec / 1e9)
    opsPerSec >= 1e6 -> "%.2f M".format(opsPerSec / 1e6)
    opsPerSec >= 1e3 -> "%.2f K".format(opsPerSec / 1e3)
    else -> "%.0f".format(opsPerSec)
}

private fun <S, R : Result> measureCell(
    spec: StatSpec<S, R>,
    level: Concurrency,
    threads: Int,
    warmupMs: Long,
    measureMs: Long,
): Cell {
    val workload = spec.updates(0, WORKLOAD_SIZE).toList().toTypedArray()
    val stat = spec.factory(level)

    runWindow(spec, stat, workload, threads, warmupMs)
    System.gc()
    val gcBefore = gcMillis()
    val ops = runWindow(spec, stat, workload, threads, measureMs)
    val gcAfter = gcMillis()
    return Cell(ops = ops.opsTotal, elapsedNanos = ops.elapsedNanos, gcMillis = gcAfter - gcBefore)
}

private data class RunResult(val opsTotal: Long, val elapsedNanos: Long)

private fun <S, R : Result> runWindow(
    spec: StatSpec<S, R>,
    stat: S,
    workload: Array<Update>,
    threads: Int,
    durationMs: Long,
): RunResult {
    if (threads == 1) {
        val deadline = System.nanoTime() + durationMs * 1_000_000L
        var ops = 0L
        val t0 = System.nanoTime()
        while (System.nanoTime() < deadline) {
            var i = 0
            while (i < workload.size) { spec.applyUpdate(stat, workload[i]); i++ }
            ops += workload.size
        }
        return RunResult(ops, System.nanoTime() - t0)
    }
    val executor = Executors.newFixedThreadPool(threads)
    val stop = AtomicBoolean(false)
    val opsTotal = AtomicLong(0)
    val start = CountDownLatch(1)
    val done = CountDownLatch(threads)
    repeat(threads) {
        executor.submit {
            start.await()
            var local = 0L
            while (!stop.get()) {
                var i = 0
                while (i < workload.size) { spec.applyUpdate(stat, workload[i]); i++ }
                local += workload.size
            }
            opsTotal.addAndGet(local)
            done.countDown()
        }
    }
    val t0 = System.nanoTime()
    start.countDown()
    Thread.sleep(durationMs)
    stop.set(true)
    check(done.await(30, TimeUnit.SECONDS)) { "${spec.name} @ $threads threads: timed out" }
    val elapsed = System.nanoTime() - t0
    executor.shutdownNow()
    return RunResult(opsTotal.get(), elapsed)
}

private fun gcMillis(): Long =
    ManagementFactory.getGarbageCollectorMXBeans().sumOf { it.collectionTime.coerceAtLeast(0L) }

private fun startRecording(path: Path, profile: String): Recording {
    Files.createDirectories(path.parent ?: Paths.get("."))
    val config = Configuration.getConfiguration(profile)
    val rec = Recording(config)
    rec.destination = path
    rec.duration = null // controlled by stop()
    rec.maxAge = Duration.ofHours(1)
    rec.start()
    return rec
}

private const val WORKLOAD_SIZE = 4096
