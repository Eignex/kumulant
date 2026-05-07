package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.Sum
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import com.eignex.kumulant.operation.atIndex as liveAtIndex
import com.eignex.kumulant.operation.withFixedX as liveWithFixedX
import com.eignex.kumulant.operation.withValue as liveWithValue
import com.eignex.kumulant.operation.withWeight as liveWithWeight

/**
 * Round-trip tests for the operation configs in [OperationConfigs.kt]. Encode,
 * decode, materialize, drive a small fixed input through both the rehydrated
 * config and the live composition, and compare results.
 */
class OperationConfigsRoundTripTest {

    private val DELTA = 1e-12

    @Test fun withWeight_series_matches_live_composition() {
        val cfg: SeriesStatConfig<SumResult> = SumConfig.withWeight(2.0)
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json)
        val rebuilt = (decoded as SeriesStatConfig<*>).materialize(Concurrency.None)
        val live = Sum().liveWithWeight(2.0)

        listOf(1.0, 3.0, 5.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun withValue_series_matches_live_composition() {
        val cfg: SeriesStatConfig<SumResult> = SumConfig.withValue(7.0)
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json)
        val rebuilt = (decoded as SeriesStatConfig<*>).materialize(Concurrency.None)
        val live = Sum().liveWithValue(7.0)

        listOf(1.0, 2.0, 3.0, 4.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun composed_chain_with_value_then_with_weight_matches_live() {
        val cfg: SeriesStatConfig<SumResult> = SumConfig.withValue(1.0).withWeight(2.0)
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json)
        val rebuilt = (decoded as SeriesStatConfig<*>).materialize(Concurrency.None)
        val live = Sum().liveWithValue(1.0).liveWithWeight(2.0)

        listOf(10.0, 20.0, 30.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        // Three updates, each adds 1.0 * 2.0 = 2.0 → sum = 6.0.
        assertEquals(6.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun atIndex_lifts_series_to_vector() {
        val cfg: VectorStatConfig<SumResult> = SumConfig.atIndex(1)
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json)
        val rebuilt = (decoded as VectorStatConfig<*>).materialize(Concurrency.None)
        val live = Sum().liveAtIndex(1)

        listOf(doubleArrayOf(1.0, 10.0), doubleArrayOf(2.0, 20.0), doubleArrayOf(3.0, 30.0)).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as SumResult
        val l = live.read()
        assertEquals(60.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun withFixedX_lifts_paired_to_series() {
        val cfg: SeriesStatConfig<com.eignex.kumulant.stat.regression.OLSResult> =
            OLSConfig.withFixedX(2.0)
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json)
        val rebuilt = (decoded as SeriesStatConfig<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.OLS().liveWithFixedX(2.0)

        listOf(4.0, 6.0, 8.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as com.eignex.kumulant.stat.regression.OLSResult
        val l = live.read()
        assertEquals(l.totalWeights, r.totalWeights, DELTA)
    }

    @Test fun windowed_series_round_trips_at_least_structurally() {
        val cfg: SeriesStatConfig<SumResult> = SumConfig.windowed(durationMillis = 1000L, slices = 4)
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json) as WindowedSeriesConfig
        assertEquals(1000L, decoded.durationMillis)
        assertEquals(4, decoded.slices)
        // Just ensure materialize doesn't throw — windowed semantics are exercised in WindowedStats own tests.
        decoded.materialize(Concurrency.None)
    }

    @Test fun vectorized_replicates_template_per_dimension() {
        val cfg: VectorStatConfig<*> = SumConfig.vectorized(dimensions = 3)
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json) as VectorizedStatConfig
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

    @Test fun asSeries_lifts_discrete() {
        val cfg: SeriesStatConfig<*> = HyperLogLogConfig(precision = 10).asSeries()
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json)
        assertIs<AsSeriesConfig>(decoded)
        val materialized = (decoded as SeriesStatConfig<*>).materialize(Concurrency.None)
        // Truncates 1.7 → 1L
        for (i in 1..50) materialized.update(i.toDouble())
        val r = materialized.read() as com.eignex.kumulant.stat.cardinality.HyperLogLogResult
        kotlin.test.assertTrue(r.estimate > 30.0)
    }

    @Test fun rejects_inner_with_wrong_modality() {
        // SumConfig is Series, but AtIndices expects Paired.
        val bad = AtIndicesConfig(SumConfig, indexX = 0, indexY = 1)
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            bad.materialize(Concurrency.None)
        }
    }
}
