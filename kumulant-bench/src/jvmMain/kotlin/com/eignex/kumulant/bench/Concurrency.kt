package com.eignex.kumulant.bench

import com.eignex.koblas.koblas
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs

internal fun analyzeConcurrency() {
    println("backend=${koblas.name}; unit-weight inputs; observations=$STAT_BENCHMARK_BATCH_SIZE")
    println("stat,level,operation,threads,count_error,scaled_drift,snapshot_reads")
    for (spec in listOf(sumStatSpec, meanStatSpec, varianceStatSpec)) {
        for (level in listOf(Concurrency.Strict, Concurrency.Relaxed, Concurrency.HighWrite)) {
            for (threads in listOf(1, 4)) for (merge in listOf(false, true)) {
                val result = measureConcurrency(spec, level, threads, merge)
                println("${spec.name},$level,${if (merge) "merge" else "update"},$threads,${result.countError},${result.drift},${result.reads}")
            }
        }
    }
}

internal data class ConcurrencyMeasurement(val countError: Double, val drift: Double, val reads: Int)

internal fun <S, R : Result> measureConcurrency(
    spec: StatSpec<S, R>, level: Concurrency, threads: Int, merge: Boolean,
    observations: Int = STAT_BENCHMARK_BATCH_SIZE,
): ConcurrencyMeasurement {
    require(level != Concurrency.None || threads == 1) { "Concurrency.None cannot be shared between writers" }
    require(observations > 0)
    val inputs = List(threads) { worker ->
        if (spec.name == "SumStat") List(observations) { Update(1.0, 1.0, 0L) }
        else uniformUnitWeights(STAT_BENCHMARK_SEED + worker, observations).toList()
    }
    val stat = spec.factory(level)
    val count = threads * observations
    val actions = if (merge) inputs.map { input ->
        val producer = spec.factory(Concurrency.None)
        input.forEach { spec.applyUpdate(producer, it) }
        val snapshot = spec.readSnapshot(producer, 0L)
        val action: () -> Unit = { spec.merge(stat, snapshot) }
        action
    } else inputs.map { input ->
        val action: () -> Unit = { input.forEach { spec.applyUpdate(stat, it) } }
        action
    }
    val reads = AtomicInteger()
    var previousCount = 0.0
    runWorkers(actions, reader = {
        val snapshot = spec.readSnapshot(stat, 0L)
        check(spec.scalar(snapshot).isFinite()) { "Nonfinite snapshot during concurrent writes" }
        if (snapshot is HasObservationCount) {
            check(snapshot.totalWeights in previousCount..count.toDouble()) { "Corrupt or decreasing observation count" }
            previousCount = snapshot.totalWeights
        }
        if (snapshot is WeightedVarianceResult) check(snapshot.mean.isFinite() && snapshot.variance.isFinite()) { "Nonfinite variance snapshot" }
        if (level != Concurrency.Relaxed) {
            if (snapshot is WeightedMeanResult) check(snapshot.mean in -1e-12..(1.0 + 1e-12)) { "Mean outside input range" }
            if (snapshot is WeightedVarianceResult) {
                check(snapshot.mean in -1e-12..(1.0 + 1e-12)) { "Mean outside input range" }
                check(snapshot.variance in -1e-12..(0.25 + 1e-12)) { "Variance outside bounded-input range" }
            }
        }
        reads.incrementAndGet()
    })
    val snapshot = spec.readSnapshot(stat, 0L)
    val actualCount = when (snapshot) {
        is HasObservationCount -> snapshot.totalWeights
        is SumResult -> snapshot.sum
        else -> error("No preservation invariant for ${spec.name}")
    }
    val reference = spec.reference(inputs.asSequence().flatten())
    val drift = abs(spec.scalar(snapshot) - reference) / maxOf(1.0, abs(reference))
    val countError = abs(actualCount - count)
    check(countError == 0.0 && drift.isFinite()) { "${spec.name}: lost observations or nonfinite drift" }
    if (level != Concurrency.Relaxed || spec.name == "SumStat") check(drift <= 1e-10) { "${spec.name}: drift=$drift" }
    return ConcurrencyMeasurement(countError, drift, reads.get())
}

internal fun runWorkers(actions: List<() -> Unit>, reader: (() -> Unit)? = null, timeoutMillis: Long = 30_000L) {
    require(actions.isNotEmpty() && timeoutMillis > 0L)
    val width = actions.size + if (reader == null) 0 else 1
    val executor = Executors.newFixedThreadPool(width)
    val completed = ExecutorCompletionService<Boolean>(executor)
    val ready = CountDownLatch(width)
    val start = CountDownLatch(1)
    val stop = AtomicBoolean()
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    try {
        actions.forEach { action -> completed.submit {
            ready.countDown()
            start.await()
            action()
            false
        } }
        val observer = reader?.let { observe -> completed.submit {
            ready.countDown()
            start.await()
            do { observe() } while (!stop.get())
            true
        } }
        check(ready.await(timeoutMillis, TimeUnit.MILLISECONDS)) { "Workers failed to become ready" }
        start.countDown()
        var finished = 0
        while (finished < actions.size) {
            val future = completed.poll(maxOf(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
                ?: error("Concurrent workload timed out")
            if (!future.get()) finished++
        }
        stop.set(true)
        observer?.get(maxOf(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
    } catch (failure: ExecutionException) {
        throw requireNotNull(failure.cause)
    } finally {
        stop.set(true)
        start.countDown()
        executor.shutdownNow()
        check(executor.awaitTermination(100, TimeUnit.MILLISECONDS)) { "Concurrent workers did not stop" }
    }
}
