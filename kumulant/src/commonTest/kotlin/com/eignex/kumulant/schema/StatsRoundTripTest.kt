package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.forecast.SeasonalMode
import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.regression.glm.Penalty
import com.eignex.kumulant.stat.summary.MaxResult
import com.eignex.kumulant.stat.summary.MinResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.skema.SchemaJson
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
class StatsRoundTripTest {

    private fun roundTrip(config: StatSpec): StatSpec {
        val json = SchemaJson.encodeToString<StatSpec>(config)
        return SchemaJson.decodeFromString<StatSpec>(json)
    }

    @Test
    fun `summary stat configurations round trip`() {
        val cases: List<StatSpec> = listOf(
            Min,
            Max,
            ArgMin,
            ArgMax,
            Range,
            Variance,
            Moments,
            Summary,
            BernoulliSum,
            Count,
        )

        for (config in cases) {
            assertEquals(config, roundTrip(config), config::class.simpleName)
        }
    }

    @Test
    fun `quantile stat configurations round trip`() {
        val cases: List<StatSpec> = listOf(
            FrugalMad(stepSize = 0.5),
            BucketHistogram(edges = listOf(0.0, 10.0, 100.0)),
            Mad(compression = 200.0),
            DDSketch(relativeError = 0.02, probabilities = listOf(0.5, 0.99)),
            FrugalQuantile(q = 0.5, stepSize = 0.02, initialEstimate = 1.0),
            ReservoirHistogram(capacity = 256, seed = 42L),
            TDigest(compression = 200.0, probabilities = listOf(0.5, 0.95)),
            PitHistogram(numBins = 20),
        )

        for (config in cases) {
            assertEquals(config, roundTrip(config), config::class.simpleName)
        }
    }

    @Test
    fun `paired stat configurations round trip`() {
        val cases: List<StatSpec> = listOf(
            PairedSum,
            UnivariateRegression(),
            Covariance,
            UnivariateRegression(Penalty.L1(lambda = 0.1)),
            UnivariateRegression(Penalty.L2(lambda = 0.5)),
            MseLoss,
            MaeLoss,
            LogLoss,
            PinballLoss(tau = 0.9),
            Auc(numBins = 128, lowerBound = -1.0, upperBound = 1.0),
            Reliability(numBins = 16),
            PlattCalibrator(),
            ConfusionMatrix(numClasses = 5),
            Accuracy(numClasses = 4),
        )

        for (config in cases) {
            assertEquals(config, roundTrip(config), config::class.simpleName)
        }
    }

    @Test
    fun `discrete stat configurations round trip`() {
        val cases: List<StatSpec> = listOf(
            HyperLogLog(precision = 12),
            BloomFilter(bits = 1024, hashes = 4),
            CountMinSketch(depth = 4, width = 512, seed = 123L),
            SpaceSaving(capacity = 32),
            Sojourn(states = listOf(0L, 1L, 2L)),
        )

        for (config in cases) {
            assertEquals(config, roundTrip(config), config::class.simpleName)
        }
    }

    @Test
    fun `temporal stat configurations round trip`() {
        val cases: List<StatSpec> = listOf(
            RunLength,
            Recency,
            Crossing(level = 3.5),
            ExponentialSmoothing(
                alpha = 0.3,
                beta = 0.1,
                phi = 0.9,
            ),
            ExponentialSmoothing(
                alpha = 0.3,
                beta = 0.1,
                phi = 0.95,
                period = 7,
                gamma = 0.2,
                mode = SeasonalMode.Multiplicative,
            ),
            Cusum(target = 1.0, referenceValue = 0.25, threshold = 4.0),
            PageHinkley(delta = 0.01, threshold = 25.0),
            Adwin(delta = 0.005, maxBucketsPerSize = 8),
            Rate,
            CounterRate(false),
            DecayingSum(60_000L),
            DecayingMean(120_000L),
            DecayingVariance(60_000L),
            EwmaMean(0.1),
            EwmaVariance(0.05),
        )

        for (config in cases) {
            assertEquals(config, roundTrip(config), config::class.simpleName)
        }
    }

    @Test fun `frugal mad materializes with center and scale`() {
        val stat = FrugalMad(stepSize = 2.0).band(3.0).materialize()
        stat.update(0.0)
        stat.update(100.0)

        assertEquals(1.0, stat.read().center)
        assertEquals(1.0, stat.read().scale)
    }

    @Test fun `frugal mad windows combine adaptive snapshots`() {
        val stat = FrugalMad().windowed(1_000).materialize()

        stat.update(5.0)

        assertEquals(5.0, stat.read().median)
        assertEquals(0.0, stat.read().mad)
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

    @Test
    fun `DDSketch spec carries a non-default indexable range through materialize`() {
        val stat = DDSketch(minIndexableValue = 1e-3, maxIndexableValue = 1e6).materialize(Concurrency.None)
        val sketch = assertIs<DDSketchStat>(stat)
        assertEquals(1e-3, sketch.minIndexableValue)
        assertEquals(1e6, sketch.maxIndexableValue)
    }
}
