package com.eignex.kumulant.schema

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Adwin
import com.eignex.kumulant.schema.spec.Auc
import com.eignex.kumulant.schema.spec.BloomFilter
import com.eignex.kumulant.schema.spec.CountMinSketch
import com.eignex.kumulant.schema.spec.CounterRate
import com.eignex.kumulant.schema.spec.Cusum
import com.eignex.kumulant.schema.spec.DDSketch
import com.eignex.kumulant.schema.spec.DiscreteStatSpec
import com.eignex.kumulant.schema.spec.FrugalQuantile
import com.eignex.kumulant.schema.spec.HdrHistogram
import com.eignex.kumulant.schema.spec.HyperLogLog
import com.eignex.kumulant.schema.spec.LinearCounting
import com.eignex.kumulant.schema.spec.Mad
import com.eignex.kumulant.schema.spec.MinHash
import com.eignex.kumulant.schema.spec.PageHinkley
import com.eignex.kumulant.schema.spec.PairedStatSpec
import com.eignex.kumulant.schema.spec.QuantileFilter
import com.eignex.kumulant.schema.spec.Reliability
import com.eignex.kumulant.schema.spec.SeriesStatSpec
import com.eignex.kumulant.schema.spec.TDigest
import com.eignex.kumulant.stat.anomaly.QuantileFilterStat
import com.eignex.kumulant.stat.calibration.ReliabilityStat
import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.cardinality.LinearCountingStat
import com.eignex.kumulant.stat.change.AdwinStat
import com.eignex.kumulant.stat.change.CusumStat
import com.eignex.kumulant.stat.change.PageHinkleyStat
import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.quantile.FrugalQuantileStat
import com.eignex.kumulant.stat.quantile.HdrHistogramStat
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.rate.CounterRateStat
import com.eignex.kumulant.stat.score.AucStat
import com.eignex.kumulant.stat.sketch.BloomFilterStat
import com.eignex.kumulant.stat.sketch.CountMinSketchStat
import com.eignex.kumulant.stat.sketch.MinHashStat
import com.eignex.kumulant.stat.summary.MadStat
import kotlin.test.Test
import kotlin.test.assertEquals

// `schema/spec/Stats.kt` declares that spec defaults match the underlying stat's primary constructor,
// so authored payloads stay terse under `encodeDefaults = false`. Nothing else enforces it, and editing
// either side alone compiles clean.
//
// The comparison is behavioural rather than field-by-field, so it needs no reflection - which common
// Kotlin cannot rely on across thirteen targets - and so it covers defaults added later.
//
// `ReservoirHistogram` is excluded: its stat defaults the seed to `Random.Default.nextLong()`, so two
// instances legitimately disagree.
class SpecDefaultParityTest {

    private val values = doubleArrayOf(0.1, 1.2, 3.7, 0.5, 2.8, 1.0, 4.5, 0.3, 2.1, 1.7, 3.0, 0.8)
    private val keys = longArrayOf(1L, 2L, 3L, 2L, 1L, 4L, 5L, 1L, 6L, 7L, 8L, 3L)
    private val stamps = LongArray(12) { it * 1_000_000_000L }

    private fun <R : Result> series(
        name: String,
        spec: SeriesStatSpec<R>,
        direct: SeriesStat<R>,
    ): Triple<String, SeriesStat<*>, SeriesStat<*>> = Triple(name, spec.materialize(), direct)

    private fun <R : Result> discrete(
        name: String,
        spec: DiscreteStatSpec<R>,
        direct: DiscreteStat<R>,
    ): Triple<String, DiscreteStat<*>, DiscreteStat<*>> = Triple(name, spec.materialize(), direct)

    private fun <R : Result> paired(
        name: String,
        spec: PairedStatSpec<R>,
        direct: PairedStat<R>,
    ): Triple<String, PairedStat<*>, PairedStat<*>> = Triple(name, spec.materialize(), direct)

    @Test
    fun `every series spec default matches the stat it materialises`() {
        val pairs = listOf(
            series("Cusum", Cusum(), CusumStat()),
            series("PageHinkley", PageHinkley(), PageHinkleyStat()),
            series("Adwin", Adwin(), AdwinStat()),
            series("Mad", Mad(), MadStat()),
            series("QuantileFilter", QuantileFilter(), QuantileFilterStat()),
            series("DDSketch", DDSketch(), DDSketchStat()),
            series("TDigest", TDigest(), TDigestStat()),
            series("HdrHistogram", HdrHistogram(), HdrHistogramStat()),
            series("CounterRate", CounterRate(), CounterRateStat()),
            // q has no default on either side, so it is supplied identically; the point here is that
            // stepSize and initialEstimate, which do, agree.
            series("FrugalQuantile", FrugalQuantile(q = 0.9), FrugalQuantileStat(q = 0.9)),
        )
        val violations = mutableListOf<String>()
        for ((name, fromSpec, direct) in pairs) {
            for (i in values.indices) {
                fromSpec.update(values[i], stamps[i], 1.0)
                direct.update(values[i], stamps[i], 1.0)
            }
            val a = fromSpec.read(stamps.last())
            val b = direct.read(stamps.last())
            if (a != b) violations += "$name: spec default gave $a, stat default gave $b"
        }
        assertEquals(emptyList(), violations.toList(), "a spec default has drifted from its stat's")
    }

    @Test
    fun `every discrete spec default matches the stat it materialises`() {
        val pairs = listOf(
            discrete("HyperLogLog", HyperLogLog(), HyperLogLogStat()),
            discrete("LinearCounting", LinearCounting(), LinearCountingStat()),
            discrete("BloomFilter", BloomFilter(), BloomFilterStat()),
            discrete("CountMinSketch", CountMinSketch(), CountMinSketchStat()),
            discrete("MinHash", MinHash(), MinHashStat()),
        )
        val violations = mutableListOf<String>()
        for ((name, fromSpec, direct) in pairs) {
            for (i in keys.indices) {
                fromSpec.update(keys[i], stamps[i], 1.0)
                direct.update(keys[i], stamps[i], 1.0)
            }
            val a = fromSpec.read(stamps.last())
            val b = direct.read(stamps.last())
            if (a != b) violations += "$name: spec default gave $a, stat default gave $b"
        }
        assertEquals(emptyList(), violations.toList(), "a spec default has drifted from its stat's")
    }

    @Test
    fun `every paired spec default matches the stat it materialises`() {
        val pairs = listOf(
            paired("Auc", Auc(), AucStat()),
            // numBins is required on both sides; included because Reliability has no other field and
            // this pins that the spec forwards it rather than substituting a default of its own.
            paired("Reliability", Reliability(numBins = 10), ReliabilityStat(numBins = 10)),
        )
        val violations = mutableListOf<String>()
        for ((name, fromSpec, direct) in pairs) {
            for (i in values.indices) {
                val p = values[i] / 5.0
                val obs = if (i % 2 == 0) 1.0 else 0.0
                fromSpec.update(p, obs, stamps[i], 1.0)
                direct.update(p, obs, stamps[i], 1.0)
            }
            val a = fromSpec.read(stamps.last())
            val b = direct.read(stamps.last())
            if (a != b) violations += "$name: spec default gave $a, stat default gave $b"
        }
        assertEquals(emptyList(), violations.toList(), "a spec default has drifted from its stat's")
    }

    @Test
    fun `the sweep can actually see a drifted default`() {
        // Guards the three sweeps above. If a read were insensitive to the defaults - because the result
        // type has identity equality, say, or because the stream never exercises the parameter - they
        // would pass no matter what drifted. Deliberately mis-set one default and confirm the same
        // comparison catches it.
        val drifted = Cusum(threshold = 999.0).materialize()
        val direct = CusumStat()
        for (i in values.indices) {
            drifted.update(values[i], stamps[i], 1.0)
            direct.update(values[i], stamps[i], 1.0)
        }

        assertEquals(
            false,
            drifted.read(stamps.last()) == direct.read(stamps.last()),
            "a deliberately wrong default went undetected, so the sweeps above prove nothing",
        )
    }
}
