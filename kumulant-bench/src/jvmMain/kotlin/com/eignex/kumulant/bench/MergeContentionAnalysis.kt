package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.abs

/**
 * Merge-under-contention report: for every [StatSpec] and every [Concurrency]
 * level, builds T per-thread snapshots, then has T threads concurrently call
 * `merge(snapshot[tid])` against a shared empty stat. The merged scalar is
 * compared against the analytical reference over the concatenation of every
 * thread's workload.
 *
 * This exercises the `merge` path under contention, separate from
 * [ConcurrencyDriftAnalysis] (which exercises `update`). Stats that are
 * exact on update under [Concurrency.Strict] are not automatically exact on
 * concurrent merge — the merge may touch coupled state with different locking
 * granularity, and some stat families take merge shortcuts that don't survive
 * interleaving.
 *
 * Invoke via `./gradlew :kumulant-bench:analyzeMergeContention`.
 */
fun main() {
    val threadCount = 4
    val updatesPerThread = 2_500

    println(
        "Merge contention report — $threadCount per-thread snapshots merged into a shared stat",
    )
    println(
        "%-32s  %-10s  %18s  %18s  %14s  %14s".format(
            "stat", "level", "merged", "reference", "abs drift", "rel drift",
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

private fun <S, R : Result> measure(
    spec: StatSpec<S, R>,
    level: Concurrency,
    threads: Int,
    updatesPerThread: Int,
) {
    val perThreadSeqs = (0 until threads).map { tid ->
        shiftForMerge(tid, updatesPerThread, spec.updates(tid, updatesPerThread)).toList()
    }

    // Build one snapshot per thread from a fresh single-threaded stat. We use
    // Concurrency.None for the producer stats so each snapshot is a clean
    // serial computation; the contention we care about is on the merge target.
    val snapshots: List<R> = perThreadSeqs.map { seq ->
        val producer = spec.factory(Concurrency.None)
        for (u in seq) spec.applyUpdate(producer, u)
        spec.readSnapshot(producer, spec.readAt(seq.size))
    }

    val target = spec.factory(level)

    if (level == Concurrency.None) {
        for (snap in snapshots) spec.merge(target, snap)
    } else {
        val executor = Executors.newFixedThreadPool(threads)
        val start = CountDownLatch(1)
        val done = CountDownLatch(threads)
        repeat(threads) { tid ->
            executor.submit {
                start.await()
                spec.merge(target, snapshots[tid])
                done.countDown()
            }
        }
        start.countDown()
        check(done.await(60, TimeUnit.SECONDS)) { "${spec.name} @ $level: timed out" }
        executor.shutdownNow()
    }

    val got = spec.scalar(spec.readSnapshot(target, spec.readAt(threads * updatesPerThread)))
    val want = spec.reference(perThreadSeqs.asSequence().flatten())
    val absDrift = abs(got - want)
    val relDrift = if (want != 0.0) absDrift / abs(want) else absDrift
    println(
        "%-32s  %-10s  %18.6g  %18.6g  %14.4g  %14.4g".format(
            spec.name, level.name, got, want, absDrift, relDrift,
        ),
    )
}

/** Same per-thread shift as the update-contention analyzer, so the reference
 *  computed over the concatenated workload matches what the merged snapshots
 *  collectively represent. */
private fun shiftForMerge(tid: Int, updatesPerThread: Int, seq: Sequence<Update>): Sequence<Update> {
    val offset = tid.toLong() * updatesPerThread.toLong() * TIME_PROGRESSING_STRIDE_NANOS
    return if (offset == 0L) seq else seq.map { Update(it.value, it.weight, it.timestampNanos + offset) }
}
