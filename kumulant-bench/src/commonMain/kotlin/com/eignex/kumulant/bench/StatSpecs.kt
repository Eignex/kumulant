@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.bench

import com.eignex.kumulant.schema.spec.Covariance
import com.eignex.kumulant.schema.spec.ResampleAggregator
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.schema.spec.Variance
import com.eignex.kumulant.schema.ops.band
import com.eignex.kumulant.schema.ops.derivative
import com.eignex.kumulant.schema.ops.diff
import com.eignex.kumulant.schema.ops.hysteresis
import com.eignex.kumulant.schema.ops.lag
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.ops.resampleByTime
import com.eignex.kumulant.schema.ops.withSelfLag
import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stat.decay.DecayingMeanStat
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.DecayingVarianceStat
import com.eignex.kumulant.stat.decay.EwmaMeanStat
import com.eignex.kumulant.stat.decay.EwmaVarianceStat
import com.eignex.kumulant.stat.forecast.HoltStat
import com.eignex.kumulant.stat.forecast.RecursiveVarianceStat
import com.eignex.kumulant.stat.forecast.SeasonalMode
import com.eignex.kumulant.stat.forecast.SeasonalSmoothingStat
import com.eignex.kumulant.stat.change.AdwinStat
import com.eignex.kumulant.stat.change.CusumStat
import com.eignex.kumulant.stat.change.PageHinkleyStat
import com.eignex.kumulant.stat.rate.CounterRateStat
import com.eignex.kumulant.stat.rate.DecayingRateStat
import com.eignex.kumulant.stat.rate.RateStat
import com.eignex.kumulant.stat.summary.ArgMaxStat
import com.eignex.kumulant.stat.summary.ArgMinStat
import com.eignex.kumulant.stat.summary.BernoulliSumStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.MadStat
import com.eignex.kumulant.stat.event.CrossingStat
import com.eignex.kumulant.stat.event.ExcursionStat
import com.eignex.kumulant.stat.summary.MaxStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MinStat
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.RangeStat
import com.eignex.kumulant.stat.event.RecencyStat
import com.eignex.kumulant.stat.event.RunLengthStat
import com.eignex.kumulant.stat.event.SojournResult
import com.eignex.kumulant.stat.event.SojournStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.quantile.ThresholdBucketStat
import com.eignex.kumulant.stat.summary.TotalWeightsStat
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.math.exp
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes

/**
 * Registry of [StatSpec]s; one entry per univariate stat. Tests and benchmarks
 * iterate over [allSpecs] (or a category subset) so adding a new stat means adding
 * an entry here and nothing else.
 */

private fun bernoulliWorkload(seed: Int, n: Int): Sequence<Update> = sequence {
    val rng = Random(seed)
    repeat(n) {
        yield(Update(if (rng.nextDouble() < 0.3) 1.0 else 0.0, 0.5 + rng.nextDouble(), 0L))
    }
}

private fun twoPassMean(data: List<Update>): Double {
    val totW = data.sumOf { it.weight }
    return if (totW == 0.0) 0.0 else data.sumOf { it.value * it.weight } / totW
}

private fun twoPassVariance(data: List<Update>): Double {
    val totW = data.sumOf { it.weight }
    if (totW == 0.0) return 0.0
    val mean = data.sumOf { it.value * it.weight } / totW
    return data.sumOf { val d = it.value - mean; it.weight * d * d } / totW
}

val sumStatSpec = seriesStatSpec(
    name = "SumStat",
    factory = { c -> SumStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { u -> u.value * u.weight } },
)

val countStatSpec = seriesStatSpec(
    name = "CountStat",
    factory = { c -> CountStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.count().toDouble() },
)

val totalWeightsStatSpec = seriesStatSpec(
    name = "TotalWeightsStat",
    factory = { c -> TotalWeightsStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { u -> u.weight } },
)

val meanStatSpec = seriesStatSpec(
    name = "MeanStat",
    factory = { c -> MeanStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
)

val varianceStatSpec = seriesStatSpec(
    name = "VarianceStat",
    factory = { c -> VarianceStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { twoPassVariance(it.toList()) },
)

val momentsStatSpec = seriesStatSpec(
    name = "MomentsStat",
    factory = { c -> MomentsStat(c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
)

val minStatSpec = seriesStatSpec(
    name = "MinStat",
    factory = { c -> MinStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.min },
    reference = { seq -> seq.fold(Double.POSITIVE_INFINITY) { acc, u -> min(acc, u.value) } },
)

val maxStatSpec = seriesStatSpec(
    name = "MaxStat",
    factory = { c -> MaxStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.max },
    reference = { seq -> seq.fold(Double.NEGATIVE_INFINITY) { acc, u -> max(acc, u.value) } },
)

val argMinStatSpec = seriesStatSpec(
    name = "ArgMinStat",
    factory = { c -> ArgMinStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.min },
    reference = { seq -> seq.fold(Double.POSITIVE_INFINITY) { acc, u -> min(acc, u.value) } },
)

val argMaxStatSpec = seriesStatSpec(
    name = "ArgMaxStat",
    factory = { c -> ArgMaxStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.max },
    reference = { seq -> seq.fold(Double.NEGATIVE_INFINITY) { acc, u -> max(acc, u.value) } },
)

val rangeStatSpec = seriesStatSpec(
    name = "RangeStat",
    factory = { c -> RangeStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.max - it.min },
    reference = { seq ->
        var lo = Double.POSITIVE_INFINITY
        var hi = Double.NEGATIVE_INFINITY
        for (u in seq) {
            if (u.value < lo) lo = u.value
            if (u.value > hi) hi = u.value
        }
        hi - lo
    },
)

val thresholdBucketStatSpec = seriesStatSpec(
    name = "ThresholdBucketStat",
    factory = { c -> ThresholdBucketStat(doubleArrayOf(0.25, 0.5, 0.75), c) },
    updates = ::uniformUnitWeights,
    scalar = { it.counts.sum() },
    reference = { it.count().toDouble() },
)

val crossingStatSpec = seriesStatSpec(
    name = "CrossingStat",
    factory = { c -> CrossingStat(level = 0.5, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { (it.upCrossings + it.downCrossings).toDouble() },
    reference = { seq ->
        var prev = -1
        var crossings = 0L
        for (u in seq) {
            val side = if (u.value >= 0.5) 1 else 0
            if (prev == -1) {
                prev = side
            } else if (prev != side) {
                crossings++
                prev = side
            }
        }
        crossings.toDouble()
    },
)

val runLengthStatSpec = seriesStatSpec(
    name = "RunLengthStat",
    factory = { c -> RunLengthStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.longest.toDouble() },
    // uniformUnitWeights yields strictly positive values, so every update is truthy
    // and the longest run equals the stream length.
    reference = { it.count().toDouble() },
)

val excursionStatSpec = seriesStatSpec(
    name = "ExcursionStat",
    factory = { c -> ExcursionStat(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.peak },
    reference = { seq -> seq.fold(Double.NEGATIVE_INFINITY) { acc, u -> max(acc, u.value) } },
)

val cusumStatSpec = seriesStatSpec(
    name = "CusumStat",
    factory = { c -> CusumStat(target = 0.5, referenceValue = 0.5, threshold = 5.0, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.cusumPositive },
    // Uniform [0, 1) with target 0.5 and k=0.5 leaves cusumPos clipped at 0 throughout.
    reference = { _ -> 0.0 },
)

val pageHinkleyStatSpec = seriesStatSpec(
    name = "PageHinkleyStat",
    factory = { c -> PageHinkleyStat(delta = 0.005, threshold = 50.0, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.mean },
    // Mean of uniform [0, 1) is 0.5.
    reference = { _ -> 0.5 },
)

val adwinStatSpec = seriesStatSpec(
    name = "AdwinStat",
    factory = { c -> AdwinStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.mean },
    // Window mean of stationary uniform [0, 1) input concentrates around 0.5.
    reference = { _ -> 0.5 },
)

val bandSeriesStatSpec = seriesStatSpec(
    name = "BandSeriesStat",
    factory = { c -> Variance.band(k = 2.0).materialize(c) },
    updates = ::uniformVariableWeights,
    // Center of a variance result is the running mean.
    scalar = { it.center },
    reference = { twoPassMean(it.toList()) },
)

val madStatSpec = seriesStatSpec(
    name = "MadStat",
    factory = { c -> MadStat(compression = 200.0, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.median },
    // Median of uniform [0, 1) is 0.5.
    reference = { _ -> 0.5 },
)

val autocorrelationStatSpec = seriesStatSpec(
    name = "Covariance.withSelfLag",
    factory = { c -> Covariance.withSelfLag(1).materialize(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.correlation },
    // For an i.i.d uniform stream the lag-1 autocorrelation tends to zero.
    reference = { _ -> 0.0 },
)

val recencyStatSpec = seriesStatSpec(
    name = "RecencyStat",
    factory = { c -> RecencyStat(c) },
    updates = ::timeProgressingUnitWeights,
    // Reference: timestamp of the most recent observation.
    scalar = { it.lastObservedTimestampNanos.toDouble() },
    reference = { seq -> seq.lastOrNull()?.timestampNanos?.toDouble() ?: Long.MIN_VALUE.toDouble() },
)

private val sojournStates = listOf(0L, 1L, 2L)

val sojournStatSpec: StatSpec<SojournStat, SojournResult> = StatSpec(
    name = "SojournStat",
    factory = { c -> SojournStat(sojournStates, c) },
    applyUpdate = { s, u ->
        // Map the workload value into the declared 3-state alphabet.
        val state = sojournStates[(u.value.toRawBits() and Long.MAX_VALUE).rem(sojournStates.size.toLong()).toInt()]
        s.update(state, u.timestampNanos, u.weight)
    },
    readSnapshot = { s, ts -> s.read(ts) },
    merge = { s, r -> s.merge(r) },
    updates = ::timeProgressingUnitWeights,
    // Total accounted time equals the span from first observation to the read timestamp.
    scalar = { it.totalNanosByState.sum().toDouble() + it.currentDwellNanos.toDouble() },
    reference = { seq ->
        val list = seq.toList()
        if (list.isEmpty()) 0.0 else timeProgressingElapsedNanos(list.size).toDouble() -
            TIME_PROGRESSING_STRIDE_NANOS.toDouble() - list.first().timestampNanos.toDouble()
    },
    readAt = { n -> timeProgressingElapsedNanos(n) - TIME_PROGRESSING_STRIDE_NANOS },
)

val lagSeriesStatSpec = seriesStatSpec(
    name = "LagSeriesStat",
    factory = { c -> Sum.lag(1).materialize(c) },
    updates = ::uniformUnitWeights,
    // Inner SumStat receives value[0..n-2]; its sum equals total - value[n-1].
    scalar = { it.sum },
    reference = { seq ->
        val list = seq.toList()
        if (list.size < 2) 0.0 else list.dropLast(1).sumOf { it.value * it.weight }
    },
)

val diffSeriesStatSpec = seriesStatSpec(
    name = "DiffSeriesStat",
    factory = { c -> Sum.diff(1).materialize(c) },
    updates = ::uniformUnitWeights,
    // First differences telescope: sum equals value[n-1] - value[0].
    scalar = { it.sum },
    reference = { seq ->
        val list = seq.toList()
        if (list.size < 2) 0.0 else list.last().value - list.first().value
    },
)

val derivativeSeriesStatSpec = seriesStatSpec(
    name = "DerivativeSeriesStat",
    factory = { c -> Sum.derivative().materialize(c) },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.sum },
    // Each derivative sample is (delta / strideSeconds). They telescope to
    // (value[n-1] - value[0]) / strideSeconds; independent of n.
    reference = { seq ->
        val list = seq.toList()
        if (list.size < 2) 0.0 else {
            val strideSec = TIME_PROGRESSING_STRIDE_NANOS / 1_000_000_000.0
            (list.last().value - list.first().value) / strideSec
        }
    },
)

val hysteresisSeriesStatSpec = seriesStatSpec(
    name = "HysteresisSeriesStat",
    factory = { c -> Sum.hysteresis(low = 0.3, high = 0.7).materialize(c) },
    updates = ::uniformUnitWeights,
    scalar = { it.sum },
    reference = { seq ->
        var state = -1 // -1 unset, 0 low, 1 high
        var acc = 0.0
        for (u in seq) {
            state = when {
                u.value > 0.7 -> 1
                u.value < 0.3 -> 0
                state == -1 -> 0
                else -> state
            }
            if (state == 1) acc += u.weight
        }
        acc
    },
)

val resampleByTimeSeriesStatSpec = seriesStatSpec(
    name = "ResampleByTimeSeriesStat",
    // Bucket length 2x the inter-update stride so each pair of updates closes a bucket.
    factory = { c ->
        Sum.resampleByTime(
            bucketMillis = (2 * TIME_PROGRESSING_STRIDE_NANOS) / 1_000_000L,
            aggregator = ResampleAggregator.Sum,
        ).materialize(c)
    },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.sum },
    // Each closed bucket forwards the sum of two consecutive values. The last (possibly
    // partial) bucket is never closed, so the reference drops its values.
    reference = { seq ->
        val list = seq.toList()
        var total = 0.0
        var i = 0
        // First update lands in bucket 1 (timestamp = STRIDE -> floorDiv(2*STRIDE) = 0).
        // Updates pair up two-at-a-time; on the third update the first bucket closes.
        // To stay schedule-agnostic just replay the operator semantics here.
        var bucketStart = -1L
        var bucketSum = 0.0
        while (i < list.size) {
            val ts = list[i].timestampNanos
            val bucket = ts.floorDiv(2 * TIME_PROGRESSING_STRIDE_NANOS)
            if (bucketStart < 0L) {
                bucketStart = bucket
                bucketSum = list[i].value
            } else if (bucket == bucketStart) {
                bucketSum += list[i].value
            } else {
                total += bucketSum
                bucketStart = bucket
                bucketSum = list[i].value
            }
            i++
        }
        total
    },
)

val pairedSumStatSpec = pairedStatSpec(
    name = "PairedSumStat",
    factory = { c -> com.eignex.kumulant.stat.summary.PairedSumStat(concurrency = c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sumX },
    reference = { it.sumOf { u -> u.value * u.weight } },
)

val bernoulliSumStatSpec = seriesStatSpec(
    name = "BernoulliSumStat",
    factory = { c -> BernoulliSumStat(c) },
    updates = ::bernoulliWorkload,
    scalar = { it.successes },
    reference = { it.sumOf { u -> u.value * u.weight } },
)

// Time-driven decay stats are exercised at `timestampNanos = 0` for every update
// and the read; the decay factor `exp(-alpha*(t - t_i))` collapses to 1 so the
// stat behaves like its non-decaying counterpart and admits a closed-form
// reference. EWMA-family stats (decay by accumulated weight) require the
// recursion-based reference and are order-dependent.

private val decayWeighting = DecayWeighting.HalfLife(1.hours)
private val ewmaWeighting = DecayWeighting.Alpha(0.01)

private fun ewmaMean(alpha: Double, data: List<Update>): Double {
    var biased = 0.0
    var cumW = 0.0
    for (u in data) {
        val a = 1.0 - exp(-alpha * u.weight)
        biased += a * (u.value - biased)
        cumW += u.weight
    }
    val bc = 1.0 - exp(-alpha * cumW)
    return if (bc > 0.0) biased / bc else 0.0
}

private fun ewmaVariance(alpha: Double, data: List<Update>): Double {
    var biasedMean = 0.0
    var biasedM2 = 0.0
    var cumW = 0.0
    for (u in data) {
        val a = 1.0 - exp(-alpha * u.weight)
        val delta = u.value - biasedMean
        biasedMean += a * delta
        biasedM2 = (1.0 - a) * (biasedM2 + a * delta * delta)
        cumW += u.weight
    }
    val bc = 1.0 - exp(-alpha * cumW)
    return if (bc > 0.0) biasedM2 / bc else 0.0
}

val decayingSumStatSpec = seriesStatSpec(
    name = "DecayingSumStat",
    factory = { c -> DecayingSumStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.sum },
    reference = { it.sumOf { u -> u.value * u.weight } },
)

val decayingMeanStatSpec = seriesStatSpec(
    name = "DecayingMeanStat",
    factory = { c -> DecayingMeanStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { twoPassMean(it.toList()) },
)

val decayingVarianceStatSpec = seriesStatSpec(
    name = "DecayingVarianceStat",
    factory = { c -> DecayingVarianceStat(decayWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { twoPassVariance(it.toList()) },
)

val ewmaMeanStatSpec = seriesStatSpec(
    name = "EwmaMeanStat",
    factory = { c -> EwmaMeanStat(ewmaWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.mean },
    reference = { ewmaMean(ewmaWeighting.alpha, it.toList()) },
)

val ewmaVarianceStatSpec = seriesStatSpec(
    name = "EwmaVarianceStat",
    factory = { c -> EwmaVarianceStat(ewmaWeighting, c) },
    updates = ::uniformVariableWeights,
    scalar = { it.variance },
    reference = { ewmaVariance(ewmaWeighting.alpha, it.toList()) },
)

private val holtAlpha = DecayWeighting.Alpha(0.3)
private val holtBeta = DecayWeighting.Alpha(0.1)

private fun holtLevelReference(seq: Sequence<Update>): Double {
    var level = 0.0
    var trend = 0.0
    var initialized = false
    for (u in seq) {
        if (!initialized) {
            level = u.value
            trend = 0.0
            initialized = true
            continue
        }
        val a = 1.0 - exp(-holtAlpha.alpha * u.weight)
        val b = 1.0 - exp(-holtBeta.alpha * u.weight)
        val prev = level
        level = a * u.value + (1.0 - a) * (prev + trend)
        trend = b * (level - prev) + (1.0 - b) * trend
    }
    return level
}

val holtStatSpec = seriesStatSpec(
    name = "HoltStat",
    factory = { c -> HoltStat(holtAlpha, holtBeta, phi = 1.0, concurrency = c) },
    updates = ::uniformVariableWeights,
    scalar = { it.level },
    reference = ::holtLevelReference,
)

private const val RV_OMEGA = 0.01
private const val RV_ALPHA = 0.1
private const val RV_BETA = 0.85

private fun recursiveVarianceReference(seq: Sequence<Update>): Double {
    var v = 0.0
    for (u in seq) v = RV_OMEGA + RV_ALPHA * u.value * u.value + RV_BETA * v
    return v
}

val recursiveVarianceStatSpec = seriesStatSpec(
    name = "RecursiveVarianceStat",
    factory = { c -> RecursiveVarianceStat(RV_OMEGA, RV_ALPHA, RV_BETA, c) },
    updates = ::uniformUnitWeights,
    scalar = { it.variance },
    reference = ::recursiveVarianceReference,
)

private val seasonalAlpha = DecayWeighting.Alpha(0.3)
private val seasonalBeta = DecayWeighting.Alpha(0.05)
private val seasonalGamma = DecayWeighting.Alpha(0.4)
private const val SEASONAL_PERIOD = 4

private fun seasonalLevelReference(seq: Sequence<Update>): Double {
    var level = 0.0
    var trend = 0.0
    val seasons = DoubleArray(SEASONAL_PERIOD)
    var initialized = false
    var slot = 0
    for (u in seq) {
        val s = seasons[slot]
        if (!initialized) {
            level = u.value
            trend = 0.0
            initialized = true
            slot = (slot + 1) % SEASONAL_PERIOD
            continue
        }
        val a = 1.0 - exp(-seasonalAlpha.alpha * u.weight)
        val b = 1.0 - exp(-seasonalBeta.alpha * u.weight)
        val g = 1.0 - exp(-seasonalGamma.alpha * u.weight)
        val prev = level
        level = a * (u.value - s) + (1.0 - a) * (prev + trend)
        trend = b * (level - prev) + (1.0 - b) * trend
        seasons[slot] = g * (u.value - level) + (1.0 - g) * s
        slot = (slot + 1) % SEASONAL_PERIOD
    }
    return level
}

val seasonalSmoothingStatSpec = seriesStatSpec(
    name = "SeasonalSmoothingStat",
    factory = { c ->
        SeasonalSmoothingStat(
            alphaWeighting = seasonalAlpha,
            betaWeighting = seasonalBeta,
            gammaWeighting = seasonalGamma,
            period = SEASONAL_PERIOD,
            mode = SeasonalMode.Additive,
            phi = 1.0,
            concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.level },
    reference = ::seasonalLevelReference,
)

// Rate stats need real elapsed time to produce meaningful values: rate =
// totalValue / elapsed_seconds. The workload progresses timestamps in 1 ms
// strides, and [StatSpec.readAt] takes the snapshot just past the last update.

// RateStat measures elapsed from the *first* observation's timestamp, not from
// zero. Our workload puts the first update at 1 ms, so elapsedSec = readAt - 1ms.
private const val WORKLOAD_STRIDE_NANOS = 1_000_000L

private fun elapsedSeconds(list: List<Update>): Double {
    if (list.isEmpty()) return 0.0
    return (readAtFor(list.size) - list.first().timestampNanos) / 1_000_000_000.0
}

private fun rateReference(seq: Sequence<Update>): Double {
    val list = seq.toList()
    val elapsedSec = elapsedSeconds(list)
    if (elapsedSec <= 0.0) return 0.0
    return list.sumOf { it.value * it.weight } / elapsedSec
}

private fun counterReference(seq: Sequence<Update>): Double {
    val list = seq.toList()
    val elapsedSec = elapsedSeconds(list)
    if (elapsedSec <= 0.0) return 0.0
    return list.last().value / elapsedSec
}

private fun readAtFor(n: Int): Long = timeProgressingElapsedNanos(n)

private val decayingRateHalfLife = 30.minutes

// DecayingRateStat exposes `decayedSum * ln(2)/halfLifeSec`. With our half-life
// (30 minutes) far exceeding the workload's elapsed window (~5–10s), the decay
// factor is ~1 so the snapshot tracks `totalValue * ln(2)/halfLifeSec`.
private fun decayingRateReference(seq: Sequence<Update>): Double {
    val list = seq.toList()
    if (list.isEmpty()) return 0.0
    val total = list.sumOf { it.value * it.weight }
    val scale = kotlin.math.ln(2.0) / (decayingRateHalfLife.inWholeNanoseconds / 1_000_000_000.0)
    return total * scale
}

val rateStatSpec = seriesStatSpec(
    name = "RateStat",
    factory = { c -> RateStat(c) },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.rate },
    reference = ::rateReference,
    readAt = ::readAtFor,
    // Under HighWrite striping, the startTimestamp may be set by a later
    // sample than the actual first, slightly shrinking the elapsed denominator.
)

val decayingRateStatSpec = seriesStatSpec(
    name = "DecayingRateStat",
    factory = { c -> DecayingRateStat(decayingRateHalfLife, c) },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.rate },
    reference = ::decayingRateReference,
    readAt = ::readAtFor,
    // Small decay over the workload window; within 1% of the un-decayed scaled sum.
)

// CounterRateStat is semantically one monotonic counter. The bench mirrors that
// by binding the factory's stat to an [AtomicLong] that serializes all writers
// behind a globally-monotonic sequence: each call to [applyUpdate] increments
// the shared counter and feeds its new value to the stat. Under concurrent
// writers the stat sees truly monotonic input regardless of thread interleaving,
// isolating the stat's own concurrency primitives from the "two independent
// counters" misuse pattern.
class CounterRateBag internal constructor(val stat: CounterRateStat, val counter: AtomicLong)

val counterRateStatSpec: StatSpec<CounterRateBag, com.eignex.kumulant.stat.rate.RateResult> = StatSpec(
    name = "CounterRateStat",
    factory = { c ->
        // Multi-writer bench: opt out of decrease-as-reset so out-of-order
        // arrivals from racing threads don't inflate the running delta. See the
        // CounterRateStat class docstring.
        CounterRateBag(CounterRateStat(c, treatDecreaseAsReset = false), AtomicLong(0L))
    },
    applyUpdate = { bag, u ->
        val i = bag.counter.addAndFetch(1L)
        bag.stat.update(i.toDouble(), u.timestampNanos, u.weight)
    },
    readSnapshot = { bag, ts -> bag.stat.read(ts) },
    merge = { bag, r -> bag.stat.merge(r) },
    updates = ::timeProgressingUnitWeights,
    scalar = { it.rate },
    reference = ::counterRateReference,
    readAt = ::readAtFor,
)

private fun counterRateReference(seq: Sequence<Update>): Double {
    val list = seq.toList()
    val elapsedSec = elapsedSeconds(list)
    if (elapsedSec <= 0.0) return 0.0
    // Each update increments the shared counter by 1, so the final absolute
    // counter value equals the stream length regardless of thread interleaving.
    return list.size.toDouble() / elapsedSec
}

// Cardinality stats consume Long identifiers. The harness converts each Update
// value to its IEEE-754 raw bits; that yields well-spread integer IDs from the
// uniform [0, 1) double workload. Reference cardinality is the count of distinct
// raw-bit IDs in the stream; sketches sit within their stated standard error.

val hyperLogLogStatSpec = discreteStatSpec(
    name = "HyperLogLogStat",
    factory = { c -> com.eignex.kumulant.stat.cardinality.HyperLogLogStat(precision = 14, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.estimate },
    reference = { seq -> seq.map { it.value.toRawBits() }.toSet().size.toDouble() },
    // Standard error ~ 1.04/sqrt(2^14) = 0.81%. With 5000 distinct IDs this is
    // about 40; allow 100 for safety across seeds and concurrency-induced drift.
)

// Sketch stats check the universal "no update was lost" invariant via totalSeen.
// The dedicated unit tests in :kumulant cover accuracy; the bench test the
// concurrency-safety of the update path.

val bloomFilterStatSpec = discreteStatSpec(
    name = "BloomFilterStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.BloomFilterStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
)

val countMinSketchStatSpec = discreteStatSpec(
    name = "CountMinSketchStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.CountMinSketchStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
)

val minHashStatSpec = discreteStatSpec(
    name = "MinHashStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.MinHashStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
)

val spaceSavingStatSpec = discreteStatSpec(
    name = "SpaceSavingStat",
    factory = { c -> com.eignex.kumulant.stat.sketch.SpaceSavingStat(capacity = 128, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
)

val linearCountingStatSpec = discreteStatSpec(
    name = "LinearCountingStat",
    factory = { c ->
        com.eignex.kumulant.stat.cardinality.LinearCountingStat(bits = 1 shl 16, concurrency = c)
    },
    updates = ::uniformUnitWeights,
    scalar = { it.estimate },
    reference = { seq -> seq.map { it.value.toRawBits() }.toSet().size.toDouble() },
    // 64K-bit bitset over 5000 distinct IDs: load ~7.6%, bias is small and the
    // estimator converges quickly. Allow 50 to be safe.
)

// Quantile stats check "no update was lost" via totalWeights / totalSeen. Frugal
// is order-dependent (random walk) and uses a wide tolerance to allow drift
// around the true median of uniform [0, 1).

val ddSketchStatSpec = seriesStatSpec(
    name = "DDSketchStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.DDSketchStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
)

val hdrHistogramStatSpec = seriesStatSpec(
    name = "HdrHistogramStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.HdrHistogramStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { snap -> snap.weights.sum() },
    reference = { it.count().toDouble() },
)

val linearHistogramStatSpec = seriesStatSpec(
    name = "LinearHistogramStat",
    factory = { c ->
        com.eignex.kumulant.stat.quantile.LinearHistogramStat(
            lowerBound = 0.0,
            upperBound = 1.0,
            binCount = 64,
            concurrency = c,
        )
    },
    updates = ::uniformUnitWeights,
    scalar = { snap -> snap.weights.sum() },
    reference = { it.count().toDouble() },
)

val reservoirHistogramStatSpec = seriesStatSpec(
    name = "ReservoirHistogramStat",
    factory = { c ->
        com.eignex.kumulant.stat.quantile.ReservoirHistogramStat(capacity = 256, concurrency = c)
    },
    updates = ::uniformUnitWeights,
    scalar = { it.totalSeen.toDouble() },
    reference = { it.count().toDouble() },
)

val tDigestStatSpec = seriesStatSpec(
    name = "TDigestStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.TDigestStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { snap -> snap.weights.sum() },
    reference = { it.count().toDouble() },
)

val frugalQuantileStatSpec = seriesStatSpec(
    name = "FrugalQuantileStat",
    factory = { c -> com.eignex.kumulant.stat.quantile.FrugalQuantileStat(q = 0.5, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.quantile },
    // Frugal is a random walk targeting q=0.5 over uniform [0,1); the median is
    // 0.5 but the estimate wanders within a few stepSizes of it.
    reference = { _ -> 0.5 },
)

// Multi-feature regression specs feed an 8-dim deterministic random feature
// vector (seeded by the update value's bits) and a known target
// y = trueWeights · x + bias. The scalar pulls weights[0] from the snapshot;
// the reference is trueWeights[0] = 2.0. This exercises the full coupled
// (Sxx, Sxy) recurrence rather than the trivial featureSize=1 path.

private const val REG_FEATURE_SIZE = 8
private val regTrueWeights = doubleArrayOf(2.0, -1.0, 0.5, -0.5, 1.0, 0.1, -0.2, 0.3)

val univariateRegressionStatSpec = pairedStatSpec(
    name = "UnivariateRegressionStat",
    factory = { c -> com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat(concurrency = c) },
    updates = ::uniformVariableWeights,
    scalar = { it.slope },
    reference = { _ -> 2.0 },
)

val covarianceStatSpec = pairedStatSpec(
    name = "CovarianceStat",
    factory = { c -> com.eignex.kumulant.stat.regression.CovarianceStat(concurrency = c) },
    updates = ::uniformVariableWeights,
    scalar = { it.covariance },
    // cov(X, 2X + 0.1) = 2 * var(X). For X ~ U[0,1), var(X) = 1/12 → cov ≈ 0.1667.
    // With weighted samples the empirical variance drifts; allow 5% slack.
    reference = { seq ->
        val data = seq.toList()
        val totW = data.sumOf { it.weight }
        val meanX = data.sumOf { it.value * it.weight } / totW
        val varX = data.sumOf { val d = it.value - meanX; it.weight * d * d } / totW
        2.0 * varX
    },
)

val bayesianRegressionStatSpec = regressionStatSpec(
    name = "BayesianRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat(
            featureSize = REG_FEATURE_SIZE, concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.weights[0] },
    reference = { _ -> regTrueWeights[0] },
    featureSize = REG_FEATURE_SIZE,
    trueWeights = regTrueWeights,
)

val diagonalRegressionStatSpec = regressionStatSpec(
    name = "DiagonalRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat(
            featureSize = REG_FEATURE_SIZE, concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.weights[0] },
    reference = { _ -> regTrueWeights[0] },
    featureSize = REG_FEATURE_SIZE,
    trueWeights = regTrueWeights,
)

val stochasticRegressionStatSpec = regressionStatSpec(
    name = "StochasticRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat(
            featureSize = REG_FEATURE_SIZE, concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.weights[0] },
    reference = { _ -> regTrueWeights[0] },
    featureSize = REG_FEATURE_SIZE,
    trueWeights = regTrueWeights,
)

// Score stats consume (prediction, label) pairs. For the bench, prediction =
// Update.value (uniform [0, 1)) and label = `deriveTargetY(x)` clamped where
// the stat needs [0, 1] inputs. We check totalWeights or a coarse score value.

private fun clamped01(x: Double): Double = x.coerceIn(0.0, 1.0)

val aucStatSpec = pairedStatSpec(
    name = "AucStat",
    factory = { c -> com.eignex.kumulant.stat.score.AucStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalPositives + it.totalNegatives },
    reference = { it.count().toDouble() },
    // AucStat takes (score, label) with label in {0, 1}. Map our deriveY to {0, 1}
    // by thresholding so the stat doesn't reject the input.
    deriveY = { if (it > 0.5) 1.0 else 0.0 },
)

val brierScoreStatSpec = pairedStatSpec(
    name = "BrierScoreStat",
    factory = { c -> com.eignex.kumulant.stat.score.BrierScoreStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
    deriveY = { if (it > 0.5) 1.0 else 0.0 },
)

val pinballLossStatSpec = pairedStatSpec(
    name = "PinballLossStat",
    factory = { c -> com.eignex.kumulant.stat.score.PinballLossStat(tau = 0.5, concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
    deriveY = ::clamped01,
)

val logLossStatSpec = pairedStatSpec(
    name = "LogLossStat",
    factory = { c -> com.eignex.kumulant.stat.score.LogLossStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
    // LogLoss needs prediction in (0, 1); shift away from the endpoints.
    deriveY = { (it * 0.98 + 0.01).coerceIn(0.001, 0.999) },
)

val maeLossStatSpec = pairedStatSpec(
    name = "MaeLossStat",
    factory = { c -> com.eignex.kumulant.stat.score.MaeLossStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
)

val mseLossStatSpec = pairedStatSpec(
    name = "MseLossStat",
    factory = { c -> com.eignex.kumulant.stat.score.MseLossStat(concurrency = c) },
    updates = ::uniformUnitWeights,
    scalar = { it.totalWeights },
    reference = { it.count().toDouble() },
)

val reliabilityStatSpec = pairedStatSpec(
    name = "ReliabilityStat",
    factory = { c -> com.eignex.kumulant.stat.calibration.ReliabilityStat(numBins = 16, concurrency = c) },
    updates = ::uniformUnitWeights,
    // Reliability tracks bin-wise counts; total weight in the snapshot's bin
    // histogram should equal the stream size.
    scalar = { snap -> snap.totalWeights.sum() },
    reference = { it.count().toDouble() },
    deriveY = { if (it > 0.5) 1.0 else 0.0 },
)

// RegressionTree regressors fold (vector, y, weight) tuples into a piecewise-constant model.
// We don't try to match slope or split structure here; the invariant is
// "every update's weight reached the snapshot", which makes
// [TreeRegressionResult.totalWeights] / [ForestRegressionResult.totalWeights]
// the natural scalar.

private val treeSplitCandidates = listOf(
    com.eignex.kumulant.stat.regression.tree.ThresholdSplit(0, 0.5),
)

val decisionTreeRegressionStatSpec = regressionStatSpec(
    name = "DecisionTreeRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = treeSplitCandidates,
            concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    scalar = { it.totalWeights },
    reference = { seq -> seq.sumOf { it.weight } },
)

val randomForestRegressionStatSpec = regressionStatSpec(
    name = "RandomForestRegressionStat",
    factory = { c ->
        com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat(
            featureSize = 1,
            splitCandidates = treeSplitCandidates,
            nbrTrees = 4,
            bagging = false,
            concurrency = c,
        )
    },
    updates = ::uniformVariableWeights,
    // ForestRegressionResult.totalWeights = sum over trees; without bagging each
    // tree absorbs the full stream, so the total is `nbrTrees * sum(weights)`.
    scalar = { it.totalWeights },
    reference = { seq -> 4.0 * seq.sumOf { it.weight } },
)

/** Every spec exposed by the bench module. */
val allSpecs: List<StatSpec<*, *>> = listOf(
    sumStatSpec,
    countStatSpec,
    totalWeightsStatSpec,
    meanStatSpec,
    varianceStatSpec,
    momentsStatSpec,
    minStatSpec,
    maxStatSpec,
    argMinStatSpec,
    argMaxStatSpec,
    rangeStatSpec,
    thresholdBucketStatSpec,
    crossingStatSpec,
    runLengthStatSpec,
    excursionStatSpec,
    recencyStatSpec,
    sojournStatSpec,
    autocorrelationStatSpec,
    madStatSpec,
    cusumStatSpec,
    pageHinkleyStatSpec,
    adwinStatSpec,
    bandSeriesStatSpec,
    lagSeriesStatSpec,
    diffSeriesStatSpec,
    derivativeSeriesStatSpec,
    hysteresisSeriesStatSpec,
    resampleByTimeSeriesStatSpec,
    bernoulliSumStatSpec,
    pairedSumStatSpec,
    decayingSumStatSpec,
    decayingMeanStatSpec,
    decayingVarianceStatSpec,
    ewmaMeanStatSpec,
    ewmaVarianceStatSpec,
    holtStatSpec,
    recursiveVarianceStatSpec,
    seasonalSmoothingStatSpec,
    rateStatSpec,
    decayingRateStatSpec,
    counterRateStatSpec,
    hyperLogLogStatSpec,
    linearCountingStatSpec,
    bloomFilterStatSpec,
    countMinSketchStatSpec,
    minHashStatSpec,
    spaceSavingStatSpec,
    ddSketchStatSpec,
    hdrHistogramStatSpec,
    linearHistogramStatSpec,
    reservoirHistogramStatSpec,
    tDigestStatSpec,
    frugalQuantileStatSpec,
    univariateRegressionStatSpec,
    covarianceStatSpec,
    bayesianRegressionStatSpec,
    diagonalRegressionStatSpec,
    stochasticRegressionStatSpec,
    aucStatSpec,
    brierScoreStatSpec,
    logLossStatSpec,
    maeLossStatSpec,
    mseLossStatSpec,
    pinballLossStatSpec,
    reliabilityStatSpec,
    decisionTreeRegressionStatSpec,
    randomForestRegressionStatSpec,
)
