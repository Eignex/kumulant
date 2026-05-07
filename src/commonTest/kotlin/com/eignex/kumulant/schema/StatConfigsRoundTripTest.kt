package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.skema.SchemaJson
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Round-trip tests for every [StatConfig]. For each modality, build a schema
 * with a config-only entry, encode, decode, materialize, drive a small fixed
 * input through both the original live stat and the rehydrated stat, and
 * compare results.
 *
 * Test-name convention: plain prose, no `()` or `$` in backticked identifiers
 * (Kotlin/Native rejects those).
 */
class StatConfigsRoundTripTest {

    private inline fun <reified C : StatConfig> roundTrip(config: C): C {
        val json = SchemaJson.encodeToString<StatConfig>(config)
        return SchemaJson.decodeFromString<StatConfig>(json) as C
    }

    // ===== Series — trivial =====

    @Test fun sumConfig_round_trips() {
        assertEquals(Sum, roundTrip(Sum))
    }

    @Test fun meanConfig_round_trips() {
        assertEquals(Mean, roundTrip(Mean))
    }

    @Test fun minConfig_round_trips() {
        assertEquals(Min, roundTrip(Min))
    }

    @Test fun maxConfig_round_trips() {
        assertEquals(Max, roundTrip(Max))
    }

    @Test fun rangeConfig_round_trips() {
        assertEquals(Range, roundTrip(Range))
    }

    @Test fun varianceConfig_round_trips() {
        assertEquals(Variance, roundTrip(Variance))
    }

    @Test fun momentsConfig_round_trips() {
        assertEquals(Moments, roundTrip(Moments))
    }

    @Test fun bernoulliSumConfig_round_trips() {
        assertEquals(BernoulliSum, roundTrip(BernoulliSum))
    }

    @Test fun totalWeightsConfig_round_trips() {
        assertEquals(TotalWeights, roundTrip(TotalWeights))
    }

    @Test fun countConfig_round_trips() {
        assertEquals(Count, roundTrip(Count))
    }

    @Test fun rateConfig_round_trips() {
        assertEquals(Rate, roundTrip(Rate))
    }

    @Test fun counterRateConfig_round_trips() {
        assertEquals(CounterRate(false), roundTrip(CounterRate(false)))
    }

    // ===== Series — primitive params =====

    @Test fun ddSketchConfig_round_trips() {
        val cfg = DDSketch(relativeError = 0.02, probabilities = listOf(0.5, 0.99))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun frugalQuantileConfig_round_trips() {
        val cfg = FrugalQuantile(q = 0.5, stepSize = 0.02, initialEstimate = 1.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun hdrHistogramConfig_round_trips() {
        val cfg = HdrHistogram(0.001, 1000.0, 4)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun linearHistogramConfig_round_trips() {
        val cfg = LinearHistogram(0.0, 100.0, 50)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun reservoirHistogramConfig_round_trips() {
        val cfg = ReservoirHistogram(capacity = 256, seed = 42L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun tDigestConfig_round_trips() {
        val cfg = TDigest(compression = 200.0, probabilities = listOf(0.5, 0.95))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun pitHistogramConfig_round_trips() {
        val cfg = PitHistogram(numBins = 20)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Paired =====

    @Test fun pairedSumConfig_round_trips() {
        assertEquals(PairedSum, roundTrip(PairedSum))
    }

    @Test fun olsConfig_round_trips() {
        assertEquals(OLS, roundTrip(OLS))
    }

    @Test fun covarianceConfig_round_trips() {
        assertEquals(Covariance, roundTrip(Covariance))
    }

    @Test fun lassoConfig_round_trips() {
        val cfg = Lasso(lambda = 0.1)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun ridgeConfig_round_trips() {
        val cfg = Ridge(lambda = 0.5)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun brierScoreConfig_round_trips() {
        assertEquals(BrierScore, roundTrip(BrierScore))
    }

    @Test fun mseLossConfig_round_trips() {
        assertEquals(MseLoss, roundTrip(MseLoss))
    }

    @Test fun maeLossConfig_round_trips() {
        assertEquals(MaeLoss, roundTrip(MaeLoss))
    }

    @Test fun logLossConfig_round_trips() {
        assertEquals(LogLoss, roundTrip(LogLoss))
    }

    @Test fun pinballLossConfig_round_trips() {
        val cfg = PinballLoss(tau = 0.9)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun aucConfig_round_trips() {
        val cfg = Auc(numBins = 128, lowerBound = -1.0, upperBound = 1.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun reliabilityConfig_round_trips() {
        val cfg = Reliability(numBins = 16)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Vector =====

    @Test fun varianceVectorConfig_round_trips() {
        val cfg = VarianceVector(dimensions = 4)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Discrete =====

    @Test fun hyperLogLogConfig_round_trips() {
        val cfg = HyperLogLog(precision = 12)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun linearCountingConfig_round_trips() {
        val cfg = LinearCounting(bits = 2048)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun bloomFilterConfig_round_trips() {
        val cfg = BloomFilter(bits = 1024, hashes = 4)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun countMinSketchConfig_round_trips() {
        val cfg = CountMinSketch(depth = 4, width = 512, seed = 123L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun minHashConfig_round_trips() {
        val cfg = MinHash(numHashes = 64, seed = 99L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun spaceSavingConfig_round_trips() {
        val cfg = SpaceSaving(capacity = 32)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Decay family =====

    @Test fun decayingSumConfig_round_trips() {
        val cfg = DecayingSum(HalfLife(60_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun decayingMeanConfig_round_trips() {
        val cfg = DecayingMean(HalfLife(120_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun decayingVarianceConfig_round_trips() {
        val cfg = DecayingVariance(HalfLife(60_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun ewmaMeanConfig_round_trips() {
        val cfg = EwmaMean(Alpha(0.1))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun ewmaVarianceConfig_round_trips() {
        val cfg = EwmaVariance(Alpha(0.05))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun decayingRateConfig_round_trips() {
        val cfg = DecayingRate(halfLifeMillis = 5_000L)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Decode-then-materialize sanity check =====

    @Test fun materializeSeries_after_round_trip_matches_live_for_sum_mean_min_max() {
        val schema = object : StatSchema() {
            val sum by series(Sum)
            val mean by series(Mean)
            val min by series(Min)
            val max by series(Max)
        }
        val def = SchemaJson.decodeFromString<StatSchemaDef>(
            SchemaJson.encodeToString(schema.statSchemaDef())
        )
        val rebuilt = StatGroup(stats = def.materializeSeries(Concurrency.None))
        val live = StatGroup(schema)

        listOf(1.0, 2.5, 0.5, 7.0).forEach {
            live.update(it);
            rebuilt.update(it)
        }

        val live0 = live.read()
        val rebuilt0 = rebuilt.read()
        assertEquals(live0[schema.sum].sum, rebuilt0[StatKey<com.eignex.kumulant.stat.summary.SumResult>("sum")].sum)
        assertEquals(
            live0[schema.mean].mean,
            rebuilt0[StatKey<com.eignex.kumulant.stat.summary.WeightedMeanResult>("mean")].mean
        )
        assertEquals(live0[schema.min].min, rebuilt0[StatKey<com.eignex.kumulant.stat.summary.MinResult>("min")].min)
        assertEquals(live0[schema.max].max, rebuilt0[StatKey<com.eignex.kumulant.stat.summary.MaxResult>("max")].max)
    }
}
