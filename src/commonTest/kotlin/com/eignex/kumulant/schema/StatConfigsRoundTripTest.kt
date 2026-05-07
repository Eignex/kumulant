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
        assertEquals(SumConfig, roundTrip(SumConfig))
    }

    @Test fun meanConfig_round_trips() {
        assertEquals(MeanConfig, roundTrip(MeanConfig))
    }

    @Test fun minConfig_round_trips() {
        assertEquals(MinConfig, roundTrip(MinConfig))
    }

    @Test fun maxConfig_round_trips() {
        assertEquals(MaxConfig, roundTrip(MaxConfig))
    }

    @Test fun rangeConfig_round_trips() {
        assertEquals(RangeConfig, roundTrip(RangeConfig))
    }

    @Test fun varianceConfig_round_trips() {
        assertEquals(VarianceConfig, roundTrip(VarianceConfig))
    }

    @Test fun momentsConfig_round_trips() {
        assertEquals(MomentsConfig, roundTrip(MomentsConfig))
    }

    @Test fun bernoulliSumConfig_round_trips() {
        assertEquals(BernoulliSumConfig, roundTrip(BernoulliSumConfig))
    }

    @Test fun totalWeightsConfig_round_trips() {
        assertEquals(TotalWeightsConfig, roundTrip(TotalWeightsConfig))
    }

    @Test fun countConfig_round_trips() {
        assertEquals(CountConfig, roundTrip(CountConfig))
    }

    @Test fun rateConfig_round_trips() {
        assertEquals(RateConfig, roundTrip(RateConfig))
    }

    @Test fun counterRateConfig_round_trips() {
        assertEquals(CounterRateConfig(false), roundTrip(CounterRateConfig(false)))
    }

    // ===== Series — primitive params =====

    @Test fun ddSketchConfig_round_trips() {
        val cfg = DDSketchConfig(relativeError = 0.02, probabilities = listOf(0.5, 0.99))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun frugalQuantileConfig_round_trips() {
        val cfg = FrugalQuantileConfig(q = 0.5, stepSize = 0.02, initialEstimate = 1.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun hdrHistogramConfig_round_trips() {
        val cfg = HdrHistogramConfig(0.001, 1000.0, 4)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun linearHistogramConfig_round_trips() {
        val cfg = LinearHistogramConfig(0.0, 100.0, 50)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun reservoirHistogramConfig_round_trips() {
        val cfg = ReservoirHistogramConfig(capacity = 256, seed = 42L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun tDigestConfig_round_trips() {
        val cfg = TDigestConfig(compression = 200.0, probabilities = listOf(0.5, 0.95))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun pitHistogramConfig_round_trips() {
        val cfg = PitHistogramConfig(numBins = 20)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Paired =====

    @Test fun pairedSumConfig_round_trips() {
        assertEquals(PairedSumConfig, roundTrip(PairedSumConfig))
    }

    @Test fun olsConfig_round_trips() {
        assertEquals(OLSConfig, roundTrip(OLSConfig))
    }

    @Test fun covarianceConfig_round_trips() {
        assertEquals(CovarianceConfig, roundTrip(CovarianceConfig))
    }

    @Test fun lassoConfig_round_trips() {
        val cfg = LassoConfig(lambda = 0.1)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun ridgeConfig_round_trips() {
        val cfg = RidgeConfig(lambda = 0.5)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun brierScoreConfig_round_trips() {
        assertEquals(BrierScoreConfig, roundTrip(BrierScoreConfig))
    }

    @Test fun mseLossConfig_round_trips() {
        assertEquals(MseLossConfig, roundTrip(MseLossConfig))
    }

    @Test fun maeLossConfig_round_trips() {
        assertEquals(MaeLossConfig, roundTrip(MaeLossConfig))
    }

    @Test fun logLossConfig_round_trips() {
        assertEquals(LogLossConfig, roundTrip(LogLossConfig))
    }

    @Test fun pinballLossConfig_round_trips() {
        val cfg = PinballLossConfig(tau = 0.9)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun aucConfig_round_trips() {
        val cfg = AucConfig(numBins = 128, lowerBound = -1.0, upperBound = 1.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun reliabilityConfig_round_trips() {
        val cfg = ReliabilityConfig(numBins = 16)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Vector =====

    @Test fun varianceVectorConfig_round_trips() {
        val cfg = VarianceVectorConfig(dimensions = 4)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun crpsGaussianConfig_round_trips() {
        assertEquals(CrpsGaussianConfig, roundTrip(CrpsGaussianConfig))
    }

    // ===== Discrete =====

    @Test fun hyperLogLogConfig_round_trips() {
        val cfg = HyperLogLogConfig(precision = 12)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun linearCountingConfig_round_trips() {
        val cfg = LinearCountingConfig(bits = 2048)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun bloomFilterConfig_round_trips() {
        val cfg = BloomFilterConfig(bits = 1024, hashes = 4)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun countMinSketchConfig_round_trips() {
        val cfg = CountMinSketchConfig(depth = 4, width = 512, seed = 123L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun minHashConfig_round_trips() {
        val cfg = MinHashConfig(numHashes = 64, seed = 99L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun spaceSavingConfig_round_trips() {
        val cfg = SpaceSavingConfig(capacity = 32)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Decay family =====

    @Test fun decayingSumConfig_round_trips() {
        val cfg = DecayingSumConfig(HalfLifeConfig(60_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun decayingMeanConfig_round_trips() {
        val cfg = DecayingMeanConfig(HalfLifeConfig(120_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun decayingVarianceConfig_round_trips() {
        val cfg = DecayingVarianceConfig(HalfLifeConfig(60_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun ewmaMeanConfig_round_trips() {
        val cfg = EwmaMeanConfig(AlphaConfig(0.1))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun ewmaVarianceConfig_round_trips() {
        val cfg = EwmaVarianceConfig(AlphaConfig(0.05))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun decayingRateConfig_round_trips() {
        val cfg = DecayingRateConfig(halfLifeMillis = 5_000L)
        assertEquals(cfg, roundTrip(cfg))
    }

    // ===== Raw =====

    @Test fun classHistogramConfig_round_trips() {
        val cfg = ClassHistogramConfig(numBins = 10, numClasses = 3)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun gradientHistogramConfig_round_trips() {
        val cfg = GradientHistogramConfig(numBins = 16)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun multiGradientHistogramConfig_round_trips() {
        val cfg = MultiGradientHistogramConfig(numFeatures = 4, numBins = 8)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun varianceHistogramConfig_round_trips() {
        val cfg = VarianceHistogramConfig(numBins = 8)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun crpsEnsembleConfig_round_trips() {
        assertEquals(CrpsEnsembleConfig, roundTrip(CrpsEnsembleConfig))
    }

    // ===== Decode-then-materialize sanity check =====

    @Test fun materializeSeries_after_round_trip_matches_live_for_sum_mean_min_max() {
        val schema = object : StatSchema() {
            val sum by series(SumConfig)
            val mean by series(MeanConfig)
            val min by series(MinConfig)
            val max by series(MaxConfig)
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

    @Test fun materializeRaw_decodes_class_histogram() {
        val cfg = ClassHistogramConfig(numBins = 5, numClasses = 2)
        val def = StatSchemaDef(stats = mapOf("hist" to cfg))
        val specs = def.materializeRaw(Concurrency.None)
        assertEquals(1, specs.size)
        assertEquals("hist", specs.single().key.name)
        assertTrue(specs.single().stat is com.eignex.kumulant.stat.tree.ClassHistogram)
    }
}
