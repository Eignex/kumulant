package com.eignex.kumulant.schema

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.schema.spec.ResampleAggregator
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import com.eignex.kumulant.operation.asDiscrete as liveAsDiscrete
import com.eignex.kumulant.operation.atIndex as liveAtIndex
import com.eignex.kumulant.operation.atIndices as liveAtIndices
import com.eignex.kumulant.operation.atX as liveAtX
import com.eignex.kumulant.operation.atY as liveAtY
import com.eignex.kumulant.operation.band as liveBand
import com.eignex.kumulant.operation.derivative as liveDerivative
import com.eignex.kumulant.operation.diff as liveDiff
import com.eignex.kumulant.operation.hysteresis as liveHysteresis
import com.eignex.kumulant.operation.lag as liveLag
import com.eignex.kumulant.operation.minMaxScaler as liveMinMaxScaler
import com.eignex.kumulant.operation.resampleByTime as liveResampleByTime
import com.eignex.kumulant.operation.standardScaler as liveStandardScaler
import com.eignex.kumulant.operation.withFeedback as liveWithFeedback
import com.eignex.kumulant.operation.withFixedX as liveWithFixedX
import com.eignex.kumulant.operation.withFixedY as liveWithFixedY
import com.eignex.kumulant.operation.withSelfLag as liveWithSelfLag
import com.eignex.kumulant.operation.withValue as liveWithValue
import com.eignex.kumulant.operation.withWeight as liveWithWeight
/**
 * Round-trip tests for the operation specs in [Operations.kt]: encode, decode,
 * materialize, drive a small fixed input, compare against a live composition.
 */

class OperationsRoundTripTest {

    @Test fun `withWeight series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.withWeight(2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveWithWeight(2.0)

        listOf(1.0, 3.0, 5.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `withValue series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.withValue(7.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveWithValue(7.0)

        listOf(1.0, 2.0, 3.0, 4.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `withValue then withWeight should compose`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.withValue(1.0).withWeight(2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveWithValue(1.0).liveWithWeight(2.0)

        listOf(10.0, 20.0, 30.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(6.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `atIndex should lift series to vector`() {
        val cfg: VectorStatSpec<SumResult> = Sum.atIndex(1)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as VectorStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveAtIndex(1)

        listOf(doubleArrayOf(1.0, 10.0), doubleArrayOf(2.0, 20.0), doubleArrayOf(3.0, 30.0)).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(60.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `withFixedX should lift paired to series`() {
        val cfg: SeriesStatSpec<UnivariateRegressionResult> =
            UnivariateRegression().withFixedX(2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat().liveWithFixedX(2.0)

        listOf(4.0, 6.0, 8.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as UnivariateRegressionResult
        val l = live.read()
        assertEquals(l.totalWeights, r.totalWeights, DELTA)
    }

    @Test fun `windowed series should round trip structurally`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.windowed(durationMillis = 1000L, slices = 4)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as WindowedSeries
        assertEquals(1000L, decoded.durationMillis)
        assertEquals(4, decoded.slices)
        decoded.materialize(Concurrency.None)
    }

    @Test fun `vectorized should replicate template per dimension`() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 3)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as Vectorized
        assertEquals(3, decoded.dimensions)
        val materialized = decoded.materialize(Concurrency.None)
        materialized.update(doubleArrayOf(1.0, 2.0, 3.0))
        materialized.update(doubleArrayOf(4.0, 5.0, 6.0))
        @Suppress("UNCHECKED_CAST")
        val rl = materialized.read() as ResultList<SumResult>
        assertEquals(3, rl.results.size)
        assertEquals(5.0, rl.results[0].sum, DELTA)
        assertEquals(7.0, rl.results[1].sum, DELTA)
        assertEquals(9.0, rl.results[2].sum, DELTA)
    }

    @Test fun `asSeries should lift discrete`() {
        val cfg: SeriesStatSpec<*> = HyperLogLog(precision = 10).asSeries()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        assertIs<AsSeries>(decoded)
        val materialized = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        for (i in 1..50) materialized.update(i.toDouble())
        val r = materialized.read() as HyperLogLogResult
        assertTrue(r.estimate > 30.0)
    }

    @Test fun `materialize should reject inner with wrong modality`() {
        val bad = AtIndices(Sum, indexX = 0, indexY = 1)
        assertFailsWith<IllegalArgumentException> {
            bad.materialize(Concurrency.None)
        }
    }

    @Test fun `withWeight paired should match live composition`() {
        val cfg: PairedStatSpec<UnivariateRegressionResult> =
            UnivariateRegression().withWeight(2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as PairedStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat().liveWithWeight(2.0)

        listOf(1.0 to 2.0, 2.0 to 4.0, 3.0 to 6.0).forEach { (x, y) ->
            rebuilt.update(x, y)
            live.update(x, y)
        }
        val r = rebuilt.read() as UnivariateRegressionResult
        val l = live.read()
        assertEquals(l.slope, r.slope, DELTA)
        assertEquals(l.totalWeights, r.totalWeights, DELTA)
    }

    @Test fun `withWeight vector should match live composition`() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2).withWeight(3.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as VectorStatSpec<*>).materialize(Concurrency.None)
        rebuilt.update(doubleArrayOf(1.0, 10.0))
        rebuilt.update(doubleArrayOf(2.0, 20.0))
        @Suppress("UNCHECKED_CAST")
        val rl = rebuilt.read() as ResultList<SumResult>
        assertEquals(9.0, rl.results[0].sum, DELTA)
        assertEquals(90.0, rl.results[1].sum, DELTA)
    }

    @Test fun `withWeight discrete should drop updates when weight is zero`() {
        val cfg: DiscreteStatSpec<*> = HyperLogLog(precision = 10).withWeight(0.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as DiscreteStatSpec<*>).materialize(Concurrency.None)
        for (i in 1L..50L) rebuilt.update(i)
        val r = rebuilt.read() as HyperLogLogResult
        assertEquals(0.0, r.estimate)
    }

    @Test fun `withValue discrete should replace every input with constant`() {
        val cfg: DiscreteStatSpec<*> = HyperLogLog(precision = 10).withValue(7L)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as DiscreteStatSpec<*>).materialize(Concurrency.None)
        for (i in 1L..100L) rebuilt.update(i)
        val r = rebuilt.read() as HyperLogLogResult
        assertTrue(r.estimate in 0.5..2.0, "estimate=${r.estimate}")
    }

    @Test fun `asDiscrete should lift series`() {
        val cfg: DiscreteStatSpec<SumResult> = Sum.asDiscrete()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        assertIs<AsDiscrete>(decoded)
        val rebuilt = (decoded as DiscreteStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveAsDiscrete()

        listOf(1L, 2L, 3L).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(6.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `atX should lift series to paired and ignore y`() {
        val cfg: PairedStatSpec<SumResult> = Sum.atX()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as PairedStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveAtX()

        listOf(1.0 to 99.0, 2.0 to 99.0, 3.0 to 99.0).forEach { (x, y) ->
            rebuilt.update(x, y)
            live.update(x, y)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(6.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `atY should lift series to paired and ignore x`() {
        val cfg: PairedStatSpec<SumResult> = Sum.atY()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as PairedStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveAtY()

        listOf(99.0 to 4.0, 99.0 to 5.0, 99.0 to 6.0).forEach { (x, y) ->
            rebuilt.update(x, y)
            live.update(x, y)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(15.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `atIndices should lift paired to vector`() {
        val cfg: VectorStatSpec<UnivariateRegressionResult> =
            UnivariateRegression().atIndices(indexX = 0, indexY = 2)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as VectorStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat().liveAtIndices(0, 2)

        // Vector slots: idx 0 is x, idx 1 ignored, idx 2 is y; y = 2x -> slope 2.
        listOf(
            doubleArrayOf(1.0, 99.0, 2.0),
            doubleArrayOf(2.0, 99.0, 4.0),
            doubleArrayOf(3.0, 99.0, 6.0),
        ).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as UnivariateRegressionResult
        val l = live.read()
        assertEquals(2.0, r.slope, DELTA)
        assertEquals(l.slope, r.slope, DELTA)
    }

    @Test fun `withFixedY should lift paired to series`() {
        val cfg: SeriesStatSpec<UnivariateRegressionResult> =
            UnivariateRegression().withFixedY(5.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat().liveWithFixedY(5.0)

        listOf(1.0, 2.0, 3.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as UnivariateRegressionResult
        val l = live.read()
        assertEquals(l.totalWeights, r.totalWeights, DELTA)
    }

    @Test fun `withTimeAsX should round trip structurally`() {
        val cfg: SeriesStatSpec<*> = UnivariateRegression().withTimeAsX()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        assertIs<WithTimeAsX>(decoded)
        decoded.materialize(Concurrency.None)
    }

    @Test fun `withTimeAsY should round trip structurally`() {
        val cfg: SeriesStatSpec<*> = UnivariateRegression().withTimeAsY()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        assertIs<WithTimeAsY>(decoded)
        decoded.materialize(Concurrency.None)
    }

    @Test fun `windowed paired should round trip structurally`() {
        val cfg: PairedStatSpec<*> = UnivariateRegression().windowed(durationMillis = 2000L, slices = 5)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as WindowedPaired
        assertEquals(2000L, decoded.durationMillis)
        assertEquals(5, decoded.slices)
        decoded.materialize(Concurrency.None)
    }

    @Test fun `windowed vector should round trip structurally`() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2).windowed(durationMillis = 500L, slices = 4)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as WindowedVector
        assertEquals(500L, decoded.durationMillis)
        assertEquals(4, decoded.slices)
        decoded.materialize(Concurrency.None)
    }

    @Test fun `windowed discrete should round trip structurally`() {
        val cfg: DiscreteStatSpec<*> = HyperLogLog(precision = 10).windowed(durationMillis = 750L, slices = 3)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as WindowedDiscrete
        assertEquals(750L, decoded.durationMillis)
        assertEquals(3, decoded.slices)
        decoded.materialize(Concurrency.None)
    }

    @Test fun `weightBy series multiplies weight by AST expression`() {
        // sum += value * weight * weighter(value), weighter = X * X.
        val cfg: SeriesStatSpec<SumResult> = Sum.weightBy(X * X)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        rebuilt.update(2.0) // 2 * 1 * 4 = 8
        rebuilt.update(3.0) // 3 * 1 * 9 = 27
        assertEquals(35.0, (rebuilt.read() as SumResult).sum, DELTA)
    }

    @Test fun `throttle series forwards every Nth update`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.throttle(every = 3)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        repeat(10) { rebuilt.update(1.0) } // updates at ticks 3, 6, 9
        assertEquals(3.0, (rebuilt.read() as SumResult).sum, DELTA)
    }

    @Test fun `lag series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.lag(2)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveLag(2)

        listOf(5.0, 7.0, 11.0, 13.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(5.0 + 7.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `diff series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.diff(1)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveDiff(1)

        listOf(1.0, 4.0, 9.0, 16.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(3.0 + 5.0 + 7.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `resampleByTime series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.resampleByTime(
            bucketMillis = 100L,
            aggregator = ResampleAggregator.Sum,
        )
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveResampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Sum)

        // Bucket 0: 1.0, 3.0 (closed by bucket-1 update). Bucket 1: 7.0 (in progress).
        val stamps = listOf(0L, 50_000_000L, 150_000_000L)
        val values = listOf(1.0, 3.0, 7.0)
        for (i in stamps.indices) {
            rebuilt.update(values[i], stamps[i])
            live.update(values[i], stamps[i])
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `withSelfLag series should match live composition`() {
        val cfg: SeriesStatSpec<*> = Covariance.withSelfLag(k = 1)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.CovarianceStat().liveWithSelfLag(k = 1)

        listOf(1.0, 2.0, 1.0, 2.0, 1.0, 2.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as CovarianceResult
        val l = live.read()
        assertEquals(l.correlation, r.correlation, DELTA)
    }

    @Test fun `withFeedback series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.withFeedback(Variance, (X - Center) / Scale)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveWithFeedback(VarianceStat(), (X - Center) / Scale)
        for (x in doubleArrayOf(1.0, 2.0, 3.0, 4.0, 5.0)) {
            rebuilt.update(x)
            live.update(x)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `standardScaler series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.standardScaler()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveStandardScaler()
        for (x in doubleArrayOf(1.0, 2.0, 3.0, 4.0, 5.0)) {
            rebuilt.update(x)
            live.update(x)
        }
        assertEquals(live.read().sum, (rebuilt.read() as SumResult).sum, DELTA)
    }

    @Test fun `minMaxScaler series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.minMaxScaler(targetLow = -1.0, targetHigh = 1.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveMinMaxScaler(targetLow = -1.0, targetHigh = 1.0)
        for (x in doubleArrayOf(2.0, 8.0, 5.0, 4.0)) {
            rebuilt.update(x)
            live.update(x)
        }
        assertEquals(live.read().sum, (rebuilt.read() as SumResult).sum, DELTA)
    }

    @Test fun `band series should match live composition`() {
        val cfg: SeriesStatSpec<BandResult> = Variance.band(k = 2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = VarianceStat().liveBand(k = 2.0)

        listOf(0.0, 1.0, 2.0, 3.0, 4.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as BandResult
        val l = live.read()
        assertEquals(l.center, r.center, DELTA)
        assertEquals(l.scale, r.scale, DELTA)
        assertEquals(l.lower, r.lower, DELTA)
        assertEquals(l.upper, r.upper, DELTA)
    }

    @Test fun `hysteresis series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.hysteresis(low = 1.0, high = 5.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveHysteresis(low = 1.0, high = 5.0)

        listOf(0.0, 2.0, 6.0, 4.0, 0.5).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `derivative series should match live composition`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.derivative()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = SumStat().liveDerivative()

        listOf(0.0 to 0L, 10.0 to 1_000_000_000L, 30.0 to 3_000_000_000L).forEach { (v, t) ->
            rebuilt.update(v, t)
            live.update(v, t)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(20.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun `sample series is deterministic across replays with the same seed`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.sample(rate = 0.5, seed = 42L)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val a = (SchemaJson.decodeFromString(StatSpec.serializer(), json) as SeriesStatSpec<*>)
            .materialize(Concurrency.None)
        val b = (SchemaJson.decodeFromString(StatSpec.serializer(), json) as SeriesStatSpec<*>)
            .materialize(Concurrency.None)
        repeat(100) {
            a.update(1.0)
            b.update(1.0)
        }
        assertEquals((a.read() as SumResult).sum, (b.read() as SumResult).sum, DELTA)
    }
}
