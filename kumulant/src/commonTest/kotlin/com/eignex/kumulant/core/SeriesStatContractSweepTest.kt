package com.eignex.kumulant.core

import com.eignex.kumulant.schema.decay.Alpha
import com.eignex.kumulant.schema.decay.HalfLife
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Adwin
import com.eignex.kumulant.schema.spec.ArgMax
import com.eignex.kumulant.schema.spec.ArgMin
import com.eignex.kumulant.schema.spec.BernoulliSum
import com.eignex.kumulant.schema.spec.Count
import com.eignex.kumulant.schema.spec.CounterRate
import com.eignex.kumulant.schema.spec.Crossing
import com.eignex.kumulant.schema.spec.Cusum
import com.eignex.kumulant.schema.spec.DDSketch
import com.eignex.kumulant.schema.spec.DecayingMean
import com.eignex.kumulant.schema.spec.DecayingRate
import com.eignex.kumulant.schema.spec.DecayingSum
import com.eignex.kumulant.schema.spec.DecayingVariance
import com.eignex.kumulant.schema.spec.EwmaMean
import com.eignex.kumulant.schema.spec.EwmaVariance
import com.eignex.kumulant.schema.spec.Excursion
import com.eignex.kumulant.schema.spec.FrugalQuantile
import com.eignex.kumulant.schema.spec.GaussianScorer
import com.eignex.kumulant.schema.spec.HdrHistogram
import com.eignex.kumulant.schema.spec.Holt
import com.eignex.kumulant.schema.spec.LinearHistogram
import com.eignex.kumulant.schema.spec.Mad
import com.eignex.kumulant.schema.spec.Max
import com.eignex.kumulant.schema.spec.Mean
import com.eignex.kumulant.schema.spec.Min
import com.eignex.kumulant.schema.spec.Moments
import com.eignex.kumulant.schema.spec.PageHinkley
import com.eignex.kumulant.schema.spec.PitHistogram
import com.eignex.kumulant.schema.spec.QuantileFilter
import com.eignex.kumulant.schema.spec.Range
import com.eignex.kumulant.schema.spec.Rate
import com.eignex.kumulant.schema.spec.Recency
import com.eignex.kumulant.schema.spec.RecursiveVariance
import com.eignex.kumulant.schema.spec.ReservoirHistogram
import com.eignex.kumulant.schema.spec.RunLength
import com.eignex.kumulant.schema.spec.SeasonalSmoothing
import com.eignex.kumulant.schema.spec.SeriesStatSpec
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.schema.spec.Summary
import com.eignex.kumulant.schema.spec.TDigest
import com.eignex.kumulant.schema.spec.ThresholdBucket
import com.eignex.kumulant.schema.spec.TotalWeights
import com.eignex.kumulant.schema.spec.Variance
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Contracts that hold for every series stat, checked against the whole catalogue rather than a
 * survey of it. The per-family tests pin behaviour one stat at a time; this sweep is what catches
 * a stat that quietly opts out of a rule the rest of the library follows.
 */
class SeriesStatContractSweepTest {

    // Every SeriesStatSpec variant, built with parameters in each stat's valid range.
    private val specs: List<Pair<String, SeriesStatSpec<*>>> = listOf(
        "Mean" to Mean,
        "Sum" to Sum,
        "Min" to Min,
        "Max" to Max,
        "ArgMin" to ArgMin,
        "ArgMax" to ArgMax,
        "Range" to Range,
        "Excursion" to Excursion,
        "RunLength" to RunLength,
        "Recency" to Recency,
        "Crossing" to Crossing(level = 5.0),
        "Cusum" to Cusum(),
        "PageHinkley" to PageHinkley(),
        "Adwin" to Adwin(),
        "Mad" to Mad(),
        "ThresholdBucket" to ThresholdBucket(thresholds = listOf(1.0, 5.0, 10.0)),
        "Variance" to Variance,
        "Moments" to Moments,
        "GaussianScorer" to GaussianScorer,
        "QuantileFilter" to QuantileFilter(),
        "Summary" to Summary,
        "BernoulliSum" to BernoulliSum,
        "TotalWeights" to TotalWeights,
        "Count" to Count,
        "Rate" to Rate,
        "CounterRate" to CounterRate(),
        "DDSketch" to DDSketch(),
        "FrugalQuantile" to FrugalQuantile(q = 0.5),
        "HdrHistogram" to HdrHistogram(),
        "LinearHistogram" to LinearHistogram(lowerBound = 0.0, upperBound = 100.0, binCount = 10),
        "ReservoirHistogram" to ReservoirHistogram(seed = 42L),
        "TDigest" to TDigest(),
        "PitHistogram" to PitHistogram(numBins = 8),
        "DecayingSum" to DecayingSum(HalfLife(30_000L)),
        "DecayingMean" to DecayingMean(HalfLife(30_000L)),
        "DecayingVariance" to DecayingVariance(HalfLife(30_000L)),
        "EwmaMean" to EwmaMean(Alpha(0.3)),
        "EwmaVariance" to EwmaVariance(Alpha(0.3)),
        "Holt" to Holt(Alpha(0.3)),
        "SeasonalSmoothing" to SeasonalSmoothing(Alpha(0.3), Alpha(0.2), Alpha(0.1), period = 4),
        "RecursiveVariance" to RecursiveVariance(omega = 0.1, alpha = 0.2, beta = 0.7),
        "DecayingRate" to DecayingRate(halfLifeMillis = 30_000L),
    )

    private val samples = doubleArrayOf(3.0, 7.5, 1.0, 12.0, 6.25, 9.0, 2.5, 11.0)
    private val readAt = 8_000_000_000L

    // A stat fed the same observations at the same timestamps, so results are comparable.
    private fun primed(spec: SeriesStatSpec<*>): SeriesStat<*> = spec.materialize().also { stat ->
        samples.forEachIndexed { i, v -> stat.update(v, i.toLong() * 1_000_000_000L, 1.0) }
    }

    @Test
    fun `a zero weight is a no-op for every series stat`() {
        // Probe above and below the sample range so one-sided stats (min versus max) are both
        // exercised, and collect every violation rather than stopping at the first.
        val violations = mutableListOf<String>()
        for ((name, spec) in specs) {
            for (probe in doubleArrayOf(999.0, -999.0)) {
                val stat = primed(spec)
                val before = stat.read(readAt)

                stat.update(probe, readAt, 0.0)

                val after = stat.read(readAt)
                if (before != after) violations += "$name absorbed a zero-weight $probe: $before -> $after"
            }
        }
        assertEquals(emptyList(), violations.toList(), "zero-weight updates must not change state")
    }

    @Test
    fun `a NaN weight is a no-op for every series stat`() {
        // The companion of the zero-weight sweep: a weight is the multiplicity of an observation and
        // NaN is not a multiplicity, so it carries nothing to fold in. This used to be three
        // different behaviours - the Welford family threw from requireLiveWeight, the sketches
        // rounded it to a weight of one, and everything else let it poison the accumulator.
        val violations = mutableListOf<String>()
        for ((name, spec) in specs) {
            for (probe in doubleArrayOf(999.0, -999.0)) {
                val stat = primed(spec)
                val before = stat.read(readAt)

                val thrown = runCatching { stat.update(probe, readAt, Double.NaN) }.exceptionOrNull()
                if (thrown != null) {
                    violations += "$name threw on a NaN weight: ${thrown.message}"
                    continue
                }

                val after = stat.read(readAt)
                if (before != after) violations += "$name absorbed a NaN-weighted $probe: $before -> $after"
            }
        }
        assertEquals(emptyList(), violations.toList(), "a NaN weight must not change state")
    }

    @Test
    fun `no series stat throws on a NaN value`() {
        // A NaN value is not filtered - it propagates, and the stat reads back NaN. The one thing
        // ruled out is turning a gap in the input into an outage in the caller, which HdrHistogram
        // did: its `value >= 0.0` check is false for NaN, so it reported a NaN as a negative value.
        val violations = mutableListOf<String>()
        for ((name, spec) in specs) {
            val stat = primed(spec)
            val thrown = runCatching { stat.update(Double.NaN, readAt, 1.0) }.exceptionOrNull()
            if (thrown != null) violations += "$name threw on a NaN value: ${thrown.message}"
            runCatching { stat.read(readAt) }.exceptionOrNull()?.let {
                violations += "$name threw reading back after a NaN value: ${it.message}"
            }
        }
        assertEquals(emptyList(), violations.toList(), "a NaN value must never throw")
    }

    @Test
    fun `a NaN value propagates through the accumulating families`() {
        // The positive half of the rule, pinned on the stats where propagation is well defined, so
        // nobody quietly reintroduces a drop guard: these must report NaN, not a stale or zero value.
        for (name in listOf("Mean", "Sum", "Variance", "Moments", "DecayingSum", "EwmaMean")) {
            val spec = specs.first { it.first == name }.second
            val stat = primed(spec)

            stat.update(Double.NaN, readAt, 1.0)

            val after = stat.read(readAt).toString()
            assertTrue(after.contains("NaN"), "$name swallowed a NaN value instead of propagating: $after")
        }
    }

    @Test
    fun `reset returns every series stat to its fresh state`() {
        for ((name, spec) in specs) {
            val stat = primed(spec)

            stat.reset()

            assertEquals(spec.materialize().read(readAt), stat.read(readAt), "$name did not reset cleanly")
        }
    }

    @Test
    fun `every series result equals itself`() {
        val violations = mutableListOf<String>()
        for ((name, spec) in specs) {
            val empty = spec.materialize()
            if (empty.read(readAt) != empty.read(readAt)) violations += "$name (empty)"
            val full = primed(spec)
            if (full.read(readAt) != full.read(readAt)) violations += "$name (primed)"
        }
        assertEquals(emptyList(), violations.toList(), "a result must equal itself")
    }

    @Test
    fun `read does not mutate any series stat`() {
        for ((name, spec) in specs) {
            val stat = primed(spec)

            val first = stat.read(readAt)

            assertEquals(first, stat.read(readAt), "$name changed as a result of being read")
        }
    }

    private companion object {
        /** Stats that deliberately act on a NaN observation; see the exception noted on [Stat]. */
        val NAN_EXEMPT = setOf("RunLength")
    }
}
