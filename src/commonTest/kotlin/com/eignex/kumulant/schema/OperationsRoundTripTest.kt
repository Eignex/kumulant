package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import com.eignex.kumulant.operation.atIndex as liveAtIndex
import com.eignex.kumulant.operation.withFixedX as liveWithFixedX
import com.eignex.kumulant.operation.withValue as liveWithValue
import com.eignex.kumulant.operation.withWeight as liveWithWeight

/**
 * Round-trip tests for the operation configs in [Operations.kt]. Encode,
 * decode, materialize, drive a small fixed input through both the rehydrated
 * config and the live composition, and compare results.
 */
private const val DELTA = 1e-12

class OperationsRoundTripTest {

    @Test fun withWeight_series_matches_live_composition() {
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

    @Test fun withValue_series_matches_live_composition() {
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

    @Test fun composed_chain_with_value_then_with_weight_matches_live() {
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
        // Three updates, each adds 1.0 * 2.0 = 2.0 → sum = 6.0.
        assertEquals(6.0, r.sum, DELTA)
        assertEquals(l.sum, r.sum, DELTA)
    }

    @Test fun atIndex_lifts_series_to_vector() {
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

    @Test fun withFixedX_lifts_paired_to_series() {
        val cfg: SeriesStatSpec<com.eignex.kumulant.stat.regression.OLSResult> =
            OLS.withFixedX(2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val rebuilt = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        val live = com.eignex.kumulant.stat.regression.OLSStat().liveWithFixedX(2.0)

        listOf(4.0, 6.0, 8.0).forEach {
            rebuilt.update(it)
            live.update(it)
        }
        val r = rebuilt.read() as com.eignex.kumulant.stat.regression.OLSResult
        val l = live.read()
        assertEquals(l.totalWeights, r.totalWeights, DELTA)
    }

    @Test fun windowed_series_round_trips_at_least_structurally() {
        val cfg: SeriesStatSpec<SumResult> = Sum.windowed(durationMillis = 1000L, slices = 4)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as WindowedSeries
        assertEquals(1000L, decoded.durationMillis)
        assertEquals(4, decoded.slices)
        // Just ensure materialize doesn't throw — windowed semantics are exercised in WindowedStats own tests.
        decoded.materialize(Concurrency.None)
    }

    @Test fun vectorized_replicates_template_per_dimension() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 3)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as VectorizedStat
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
        val cfg: SeriesStatSpec<*> = HyperLogLog(precision = 10).asSeries()
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        assertIs<AsSeries>(decoded)
        val materialized = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        // Truncates 1.7 → 1L
        for (i in 1..50) materialized.update(i.toDouble())
        val r = materialized.read() as com.eignex.kumulant.stat.cardinality.HyperLogLogResult
        kotlin.test.assertTrue(r.estimate > 30.0)
    }

    @Test fun rejects_inner_with_wrong_modality() {
        // Sum is Series, but AtIndices expects Paired.
        val bad = AtIndices(Sum, indexX = 0, indexY = 1)
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            bad.materialize(Concurrency.None)
        }
    }
}
