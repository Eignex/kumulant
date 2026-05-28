package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.abs

/**
 * Concurrency drift report: for every [StatSpec] and every [Concurrency] level,
 * drives N threads × M updates against a shared stat, then prints the snapshot
 * scalar alongside the analytical reference and the absolute drift.
 *
 * Useful for inspecting how each level trades safety for cost:
 *  - [Concurrency.None] (serial); establishes the reference.
 *  - [Concurrency.Relaxed]; drift permitted on coupled stats; this report
 *    quantifies it.
 *  - [Concurrency.Strict]; locked; drift should be the same as serial up to
 *    floating-point reorder ULPs.
 *  - [Concurrency.HighWrite]; striped adders on JVM; same as Strict for
 *    additive stats, drift may appear on non-additive ones.
 *
 * Per-thread workloads are shifted onto disjoint time windows so rate stats that
 * drop out-of-order timestamps (CounterRate) see a globally monotonic stream.
 *
 * Invoke via `./gradlew :kumulant-bench:analyzeConcurrencyDrift`.
 */
fun main() {
    val threadCount = 4
    val updatesPerThread = 2_500
    val total = threadCount * updatesPerThread

    println("Concurrency drift report; $threadCount threads × $updatesPerThread updates each ($total total)")
    println(
        "%-32s  %-10s  %18s  %18s  %14s  %14s".format(
            "stat", "level", "snapshot", "reference", "abs drift", "rel drift",
        ),
    )
    println("-".repeat(125))

    for (spec in allSpecs) {
        for (level in Concurrency.entries) {
            measure(spec, level, threadCount, updatesPerThread)
        }
        println()
    }
}

/** Shift thread [tid]'s update timestamps onto its own non-overlapping window. */
private fun shiftForThread(tid: Int, updatesPerThread: Int, seq: Sequence<Update>): Sequence<Update> {
    val offset = tid.toLong() * updatesPerThread.toLong() * TIME_PROGRESSING_STRIDE_NANOS
    return if (offset == 0L) seq else seq.map { Update(it.value, it.weight, it.timestampNanos + offset) }
}

private fun <S, R : Result> measure(
    spec: StatSpec<S, R>,
    level: Concurrency,
    threads: Int,
    updatesPerThread: Int,
) {
    val stat = spec.factory(level)
    val seqs = (0 until threads).map { tid ->
        shiftForThread(tid, updatesPerThread, spec.updates(tid, updatesPerThread)).toList()
    }

    if (level == Concurrency.None) {
        for (s in seqs) for (u in s) spec.applyUpdate(stat, u)
    } else {
        val executor = Executors.newFixedThreadPool(threads)
        val start = CountDownLatch(1)
        val done = CountDownLatch(threads)
        repeat(threads) { tid ->
            executor.submit {
                start.await()
                for (u in seqs[tid]) spec.applyUpdate(stat, u)
                done.countDown()
            }
        }
        start.countDown()
        check(done.await(60, TimeUnit.SECONDS)) { "${spec.name} @ $level: timed out" }
        executor.shutdownNow()
    }

    val got = spec.scalar(spec.readSnapshot(stat, spec.readAt(threads * updatesPerThread)))
    val want = spec.reference(seqs.asSequence().flatten())
    val absDrift = abs(got - want)
    val relDrift = if (want != 0.0) absDrift / abs(want) else absDrift
    println(
        "%-32s  %-10s  %18.6g  %18.6g  %14.4g  %14.4g".format(
            spec.name, level.name, got, want, absDrift, relDrift,
        ),
    )
}
