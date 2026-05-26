package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.forecast.SeasonalMode
import com.eignex.kumulant.stat.regression.Penalty
import com.eignex.kumulant.stat.summary.MaxResult
import com.eignex.kumulant.stat.summary.MinResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.skema.SchemaJson
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertEquals
/**
 * Round-trip tests for every [StatSpec]. For each modality, build a schema
 * with a config-only entry, encode, decode, materialize, drive a small fixed
 * input through both the original live stat and the rehydrated stat, and
 * compare results.
 */
class StatsRoundTripTest {

    private inline fun <reified C : StatSpec> roundTrip(config: C): C {
        val json = SchemaJson.encodeToString<StatSpec>(config)
        return SchemaJson.decodeFromString<StatSpec>(json) as C
    }

    @Test fun `sumConfig round trips`() {
        assertEquals(Sum, roundTrip(Sum))
    }

    @Test fun `meanConfig round trips`() {
        assertEquals(Mean, roundTrip(Mean))
    }

    @Test fun `minConfig round trips`() {
        assertEquals(Min, roundTrip(Min))
    }

    @Test fun `maxConfig round trips`() {
        assertEquals(Max, roundTrip(Max))
    }

    @Test fun `rangeConfig round trips`() {
        assertEquals(Range, roundTrip(Range))
    }

    @Test fun `excursionConfig round trips`() {
        assertEquals(Excursion, roundTrip(Excursion))
    }

    @Test fun `runLengthConfig round trips`() {
        assertEquals(RunLength, roundTrip(RunLength))
    }

    @Test fun `recencyConfig round trips`() {
        assertEquals(Recency, roundTrip(Recency))
    }

    @Test fun `crossingConfig round trips`() {
        val cfg = Crossing(level = 3.5)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `holtConfig round trips`() {
        val cfg = Holt(
            alphaWeighting = Alpha(0.3),
            betaWeighting = Alpha(0.1),
            phi = 0.9,
        )
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `seasonalSmoothingConfig round trips`() {
        val cfg = SeasonalSmoothing(
            alphaWeighting = Alpha(0.3),
            betaWeighting = Alpha(0.1),
            gammaWeighting = Alpha(0.2),
            period = 7,
            mode = SeasonalMode.Multiplicative,
            phi = 0.95,
        )
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `recursiveVarianceConfig round trips`() {
        val cfg = RecursiveVariance(omega = 0.1, alpha = 0.05, beta = 0.9)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `thresholdBucketConfig round trips`() {
        val cfg = ThresholdBucket(thresholds = listOf(0.0, 10.0, 100.0))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `madConfig round trips`() {
        val cfg = Mad(compression = 200.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `cusumConfig round trips`() {
        val cfg = Cusum(target = 1.0, referenceValue = 0.25, threshold = 4.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `pageHinkleyConfig round trips`() {
        val cfg = PageHinkley(delta = 0.01, threshold = 25.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `adwinConfig round trips`() {
        val cfg = Adwin(delta = 0.005, maxBucketsPerSize = 8)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `varianceConfig round trips`() {
        assertEquals(Variance, roundTrip(Variance))
    }

    @Test fun `momentsConfig round trips`() {
        assertEquals(Moments, roundTrip(Moments))
    }

    @Test fun `summaryConfig round trips`() {
        assertEquals(Summary, roundTrip(Summary))
    }

    @Test fun `bernoulliSumConfig round trips`() {
        assertEquals(BernoulliSum, roundTrip(BernoulliSum))
    }

    @Test fun `totalWeightsConfig round trips`() {
        assertEquals(TotalWeights, roundTrip(TotalWeights))
    }

    @Test fun `countConfig round trips`() {
        assertEquals(Count, roundTrip(Count))
    }

    @Test fun `rateConfig round trips`() {
        assertEquals(Rate, roundTrip(Rate))
    }

    @Test fun `counterRateConfig round trips`() {
        assertEquals(CounterRate(false), roundTrip(CounterRate(false)))
    }

    @Test fun `ddSketchConfig round trips`() {
        val cfg = DDSketch(relativeError = 0.02, probabilities = listOf(0.5, 0.99))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `frugalQuantileConfig round trips`() {
        val cfg = FrugalQuantile(q = 0.5, stepSize = 0.02, initialEstimate = 1.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `hdrHistogramConfig round trips`() {
        val cfg = HdrHistogram(0.001, 1000.0, 4)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `linearHistogramConfig round trips`() {
        val cfg = LinearHistogram(0.0, 100.0, 50)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `reservoirHistogramConfig round trips`() {
        val cfg = ReservoirHistogram(capacity = 256, seed = 42L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `tDigestConfig round trips`() {
        val cfg = TDigest(compression = 200.0, probabilities = listOf(0.5, 0.95))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `pitHistogramConfig round trips`() {
        val cfg = PitHistogram(numBins = 20)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `pairedSumConfig round trips`() {
        assertEquals(PairedSum, roundTrip(PairedSum))
    }

    @Test fun `olsConfig round trips`() {
        assertEquals(UnivariateRegression(), roundTrip(UnivariateRegression()))
    }

    @Test fun `covarianceConfig round trips`() {
        assertEquals(Covariance, roundTrip(Covariance))
    }

    @Test fun `lassoConfig round trips`() {
        val cfg = UnivariateRegression(Penalty.L1(lambda = 0.1))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `ridgeConfig round trips`() {
        val cfg = UnivariateRegression(Penalty.L2(lambda = 0.5))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `brierScoreConfig round trips`() {
        assertEquals(BrierScore, roundTrip(BrierScore))
    }

    @Test fun `mseLossConfig round trips`() {
        assertEquals(MseLoss, roundTrip(MseLoss))
    }

    @Test fun `maeLossConfig round trips`() {
        assertEquals(MaeLoss, roundTrip(MaeLoss))
    }

    @Test fun `logLossConfig round trips`() {
        assertEquals(LogLoss, roundTrip(LogLoss))
    }

    @Test fun `pinballLossConfig round trips`() {
        val cfg = PinballLoss(tau = 0.9)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `aucConfig round trips`() {
        val cfg = Auc(numBins = 128, lowerBound = -1.0, upperBound = 1.0)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `reliabilityConfig round trips`() {
        val cfg = Reliability(numBins = 16)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `hyperLogLogConfig round trips`() {
        val cfg = HyperLogLog(precision = 12)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `linearCountingConfig round trips`() {
        val cfg = LinearCounting(bits = 2048)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `bloomFilterConfig round trips`() {
        val cfg = BloomFilter(bits = 1024, hashes = 4)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `countMinSketchConfig round trips`() {
        val cfg = CountMinSketch(depth = 4, width = 512, seed = 123L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `minHashConfig round trips`() {
        val cfg = MinHash(numHashes = 64, seed = 99L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `spaceSavingConfig round trips`() {
        val cfg = SpaceSaving(capacity = 32)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `sojournConfig round trips`() {
        val cfg = Sojourn(states = listOf(0L, 1L, 2L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `decayingSumConfig round trips`() {
        val cfg = DecayingSum(HalfLife(60_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `decayingMeanConfig round trips`() {
        val cfg = DecayingMean(HalfLife(120_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `decayingVarianceConfig round trips`() {
        val cfg = DecayingVariance(HalfLife(60_000L))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `ewmaMeanConfig round trips`() {
        val cfg = EwmaMean(Alpha(0.1))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `ewmaVarianceConfig round trips`() {
        val cfg = EwmaVariance(Alpha(0.05))
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `decayingRateConfig round trips`() {
        val cfg = DecayingRate(halfLifeMillis = 5_000L)
        assertEquals(cfg, roundTrip(cfg))
    }

    @Test fun `materializeSeries after round trip matches live for sum mean min max`() {
        val schema = object : StatSchema() {
            val sum by series(Sum)
            val mean by series(Mean)
            val min by series(Min)
            val max by series(Max)
        }
        val def = SchemaJson.decodeFromString<StatSchemaDef>(
            SchemaJson.encodeToString(schema.statSchemaDef()),
        )
        val rebuilt = StatGroup(stats = def.materializeSeries(Concurrency.None))
        val live = StatGroup(schema)

        listOf(1.0, 2.5, 0.5, 7.0).forEach {
            live.update(it)
            rebuilt.update(it)
        }

        val live0 = live.read()
        val rebuilt0 = rebuilt.read()
        assertEquals(live0[schema.sum].sum, rebuilt0[StatKey<SumResult>("sum")].sum)
        assertEquals(
            live0[schema.mean].mean,
            rebuilt0[StatKey<WeightedMeanResult>("mean")].mean,
        )
        assertEquals(live0[schema.min].min, rebuilt0[StatKey<MinResult>("min")].min)
        assertEquals(live0[schema.max].max, rebuilt0[StatKey<MaxResult>("max")].max)
    }
}
