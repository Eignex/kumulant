package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import com.eignex.kumulant.operation.asDiscrete as liveAsDiscrete
import com.eignex.kumulant.operation.atIndex as liveAtIndex
import com.eignex.kumulant.operation.atIndices as liveAtIndices
import com.eignex.kumulant.operation.atX as liveAtX
import com.eignex.kumulant.operation.atY as liveAtY
import com.eignex.kumulant.operation.withFixedX as liveWithFixedX
import com.eignex.kumulant.operation.withFixedY as liveWithFixedY
import com.eignex.kumulant.operation.withValue as liveWithValue
import com.eignex.kumulant.operation.withWeight as liveWithWeight

/**
 * Round-trip tests for the operation specs in [Operations.kt]: encode, decode,
 * materialize, drive a small fixed input, compare against a live composition.
 */
private const val DELTA = 1e-12

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
        val cfg: SeriesStatSpec<com.eignex.kumulant.stat.regression.UnivariateRegressionResult> =
            UnivariateRegression().withFixedX(2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.UnivariateRegressionStat().liveWithFixedX(2.0)

        listOf(4.0, 6.0, 8.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as com.eignex.kumulant.stat.regression.UnivariateRegressionResult
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
        val rl = materialized.read() as com.eignex.kumulant.core.ResultList<SumResult>
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
        val r = materialized.read() as com.eignex.kumulant.stat.cardinality.HyperLogLogResult
        kotlin.test.assertTrue(r.estimate > 30.0)
    }

    @Test fun `materialize should reject inner with wrong modality`() {
        val bad = AtIndices(Sum, indexX = 0, indexY = 1)
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            bad.materialize(Concurrency.None)
        }
    }

    @Test fun `withWeight paired should match live composition`() {
        val cfg: PairedStatSpec<com.eignex.kumulant.stat.regression.UnivariateRegressionResult> =
            UnivariateRegression().withWeight(2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as PairedStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.UnivariateRegressionStat().liveWithWeight(2.0)

        listOf(1.0 to 2.0, 2.0 to 4.0, 3.0 to 6.0).forEach { (x, y) ->
            rebuilt.update(x, y)
            live.update(x, y)
        }
        val r = rebuilt.read() as com.eignex.kumulant.stat.regression.UnivariateRegressionResult
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
        val rl = rebuilt.read() as com.eignex.kumulant.core.ResultList<SumResult>
        assertEquals(9.0, rl.results[0].sum, DELTA)
        assertEquals(90.0, rl.results[1].sum, DELTA)
    }

    @Test fun `withWeight discrete should drop updates when weight is zero`() {
        val cfg: DiscreteStatSpec<*> = HyperLogLog(precision = 10).withWeight(0.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as DiscreteStatSpec<*>).materialize(Concurrency.None)
        for (i in 1L..50L) rebuilt.update(i)
        val r = rebuilt.read() as com.eignex.kumulant.stat.cardinality.HyperLogLogResult
        assertEquals(0.0, r.estimate)
    }

    @Test fun `withValue discrete should replace every input with constant`() {
        val cfg: DiscreteStatSpec<*> = HyperLogLog(precision = 10).withValue(7L)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as DiscreteStatSpec<*>).materialize(Concurrency.None)
        for (i in 1L..100L) rebuilt.update(i)
        val r = rebuilt.read() as com.eignex.kumulant.stat.cardinality.HyperLogLogResult
        kotlin.test.assertTrue(r.estimate in 0.5..2.0, "estimate=${r.estimate}")
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
        val cfg: VectorStatSpec<com.eignex.kumulant.stat.regression.UnivariateRegressionResult> =
            UnivariateRegression().atIndices(indexX = 0, indexY = 2)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as VectorStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.UnivariateRegressionStat().liveAtIndices(0, 2)

        // Vector slots: idx 0 is x, idx 1 ignored, idx 2 is y; y = 2x -> slope 2.
        listOf(
            doubleArrayOf(1.0, 99.0, 2.0),
            doubleArrayOf(2.0, 99.0, 4.0),
            doubleArrayOf(3.0, 99.0, 6.0),
        ).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as com.eignex.kumulant.stat.regression.UnivariateRegressionResult
        val l = live.read()
        assertEquals(2.0, r.slope, DELTA)
        assertEquals(l.slope, r.slope, DELTA)
    }

    @Test fun `withFixedY should lift paired to series`() {
        val cfg: SeriesStatSpec<com.eignex.kumulant.stat.regression.UnivariateRegressionResult> =
            UnivariateRegression().withFixedY(5.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.UnivariateRegressionStat().liveWithFixedY(5.0)

        listOf(1.0, 2.0, 3.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as com.eignex.kumulant.stat.regression.UnivariateRegressionResult
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
}
