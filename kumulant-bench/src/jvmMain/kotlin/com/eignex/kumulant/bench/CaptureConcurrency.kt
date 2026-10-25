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

internal fun concurrencyCaptureQuality(case: CaptureCase): CaptureValidation =
    concurrencyCaptureQuality(allSpecs.single { it.name == case.workloadParameters.getValue("name") }, case)

private fun <S, R : Result> concurrencyCaptureQuality(spec: StatSpec<S, R>, case: CaptureCase): CaptureValidation {
    val level = Concurrency.valueOf(case.workloadParameters.getValue("concurrency"))
    require(level != Concurrency.None || case.threads == 1) { "Concurrency.None cannot be shared between writers" }
    val observations = case.workloadParameters.getValue("observations").toInt()
    require(observations > 0)
    val inputs = List(case.threads) { worker ->
        if (spec.name == "SumStat") List(observations) { Update(1.0, 1.0, 0L) }
        else uniformUnitWeights(case.seed + worker, observations).toList()
    }
    val fingerprint = CaptureFingerprint()
    inputs.flatten().forEach { fingerprint.add(it.value.toRawBits()); fingerprint.add(it.weight.toRawBits()); fingerprint.add(it.timestampNanos) }
    val stat = spec.factory(level)
    val count = case.threads * observations
    val actions = if (case.workloadParameters.getValue("operation") == "merge") inputs.map { input ->
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
    runCaptureWorkers(actions, reader = {
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
    return CaptureValidation(koblas.name, fingerprint.finish(), listOf(
        CaptureMetric("observationCountError", abs(actualCount - count), "observations", 0.0),
        CaptureMetric("scaledDrift", drift, "ratio", if (level == Concurrency.Relaxed && spec.name != "SumStat") null else 1e-10),
        CaptureMetric("snapshotReads", reads.get().toDouble(), "reads"),
    ))
}

internal fun runCaptureWorkers(actions: List<() -> Unit>, reader: (() -> Unit)? = null, timeoutMillis: Long = 30_000L) {
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
