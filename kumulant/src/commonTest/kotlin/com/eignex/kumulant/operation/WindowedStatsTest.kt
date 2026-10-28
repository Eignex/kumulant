package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds
import com.eignex.kumulant.schema.ops.diff as specDiff
import com.eignex.kumulant.schema.ops.transform as specTransform
import com.eignex.kumulant.schema.ops.windowed as specWindowed

private const val T0 = 0L
private const val T3 = 3_000_000_000L
private const val T9 = 9_000_000_000L
private const val T10 = 10_000_000_000L
private const val T11 = 11_000_000_000L
private const val T20 = 20_000_000_000L

class WindowedStatsTest {

    private fun sumWindowed(slices: Int = 10) = SumStat().windowed(duration = 10.seconds, slices = slices)

    @Test
    fun `update within window is included in read`() {
        val w = sumWindowed()
        w.update(5.0, T3)
        assertEquals(5.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `update older than window is excluded`() {
        val w = sumWindowed()
        w.update(100.0, T0)
        w.update(5.0, T10)
        assertEquals(5.0, w.read(T11).sum, DELTA)
    }

    @Test
    fun `multiple updates in same slice accumulate`() {
        val w = sumWindowed()
        w.update(3.0, T3)
        w.update(4.0, T3)
        w.update(2.0, T3)
        assertEquals(9.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `updates in different slices all included when within window`() {
        val w = sumWindowed()
        w.update(1.0, T3)
        w.update(2.0, T9)
        assertEquals(3.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `all updates expire after full window elapses`() {
        val w = sumWindowed()
        w.update(99.0, T0)
        w.update(99.0, T3)
        w.update(99.0, T9)
        assertEquals(0.0, w.read(T20).sum, DELTA)
    }

    @Test
    fun `slot rotation evicts stale data from reused bucket`() {
        val w = sumWindowed()
        w.update(100.0, T0)
        w.update(7.0, T10)
        assertEquals(7.0, w.read(T11).sum, DELTA)
    }

    @Test
    fun `late out-of-order event is dropped`() {
        val w = sumWindowed()
        w.update(5.0, T10)
        w.update(99.0, T0)
        assertEquals(5.0, w.read(T11).sum, DELTA)
    }

    @Test
    fun `reset clears all buckets`() {
        val w = sumWindowed()
        w.update(10.0, T3)
        w.update(20.0, T9)
        w.reset()
        assertEquals(0.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val w1 = sumWindowed()
        w1.update(10.0, T3)
        val w2 = w1.create()
        w2.update(5.0, T3)
        assertEquals(10.0, w1.read(T9).sum, DELTA)
        assertEquals(5.0, w2.read(T9).sum, DELTA)
    }

    @Test
    fun `works with MeanStat stat`() {
        val w = MeanStat().windowed(duration = 10.seconds)
        w.update(4.0, T3)
        w.update(6.0, T9)
        assertEquals(5.0, w.read(T9).mean, DELTA)
    }

    @Test
    fun `single slice covers entire window`() {
        val w = SumStat().windowed(duration = 10.seconds, slices = 1)
        w.update(3.0, T0)
        w.update(4.0, T3)
        assertEquals(7.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `weighted update is respected`() {
        val w = sumWindowed()
        w.update(1.0, T3, weight = 5.0)
        assertEquals(5.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `read excludes slots newer than the read timestamp`() {
        val w = sumWindowed()
        w.update(7.0, T10)
        assertEquals(0.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `update exactly at cutoff boundary is included`() {
        val w = sumWindowed()
        w.update(8.0, T0)
        assertEquals(8.0, w.read(T10).sum, DELTA)
    }

    @Test
    fun `paired windowed works with axis selection`() {
        val w = SumStat().atX().windowed(duration = 10.seconds, slices = 10)
        w.update(2.0, 100.0, T3)
        w.update(3.0, 200.0, T9)
        assertEquals(5.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `vector windowed works with index selection`() {
        val w = SumStat().atIndex(1).windowed(duration = 10.seconds, slices = 10)
        w.update(doubleArrayOf(1.0, 4.0), T3)
        w.update(doubleArrayOf(1.0, 6.0), T9)
        assertEquals(10.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `discrete windowed only counts in-window keys`() {
        val w = com.eignex.kumulant.stat.cardinality.LinearCountingStat(bits = 4096)
            .windowed(duration = 10.seconds)
        w.update(1L, T0)
        w.update(2L, T3)
        w.update(3L, T9)
        w.update(4L, T10)
        val seen = w.read(T11).estimate
        assertTrue(seen in 2.0..4.0, "estimate=$seen")
    }

    @Test
    fun `windowed rejects invalid configuration`() {
        assertFailsWith<IllegalArgumentException> {
            SumStat().windowed(duration = Duration.ZERO)
        }
        assertFailsWith<IllegalArgumentException> {
            SumStat().windowed(duration = (-1).nanoseconds)
        }
        assertFailsWith<IllegalArgumentException> {
            SumStat().windowed(duration = 10.seconds, slices = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            SumStat().windowed(duration = 1.nanoseconds, slices = 2)
        }
    }

    @Test
    fun `a stateful operator inside a window keeps its state across slice rotations`() {
        val windowed = SumStat().diff(1).windowed(60.seconds)
        val plain = SumStat().diff(1)
        var t = 0L
        repeat(6) {
            windowed.update(it.toDouble(), t)
            plain.update(it.toDouble(), t)
            t += 8_000_000_000L
        }
        assertEquals(plain.read(t).sum, windowed.read(t).sum, DELTA)
    }

    @Test
    fun `updates resume after gaps longer than the window`() {
        for (concurrency in Concurrency.entries) {
            val stat = SumStat(concurrency).windowed(10.seconds, concurrency = concurrency)
            stat.update(1.0, T0)
            stat.update(7.0, T11)
            stat.update(9.0, T20)

            assertEquals(16.0, stat.read(T20).sum, DELTA, "mode=$concurrency")
        }
    }

    @Test
    fun `a distant forward timestamp contributes to its window`() {
        val stat = sumWindowed()
        val future = 60_003_000_000_000L
        stat.update(1.0, T0)
        stat.update(5.0, future)
        stat.update(7.0, future + 1_000_000_000L)

        assertEquals(12.0, stat.read(future + 1_000_000_000L).sum, DELTA)
    }

    @Test
    fun `identity transform preserves differences across window slices`() {
        val stat = Sum.specDiff().specTransform(X).specWindowed(60_000L).materialize()
        repeat(6) { stat.update(it.toDouble(), it * 8_000_000_000L) }

        assertEquals(5.0, stat.read(40_000_000_000L).sum, DELTA)
    }

    @Test
    fun `series decorators preserve differences across window slices`() {
        val decorators: List<(SeriesStat<SumResult>) -> SeriesStat<SumResult>> = listOf(
            { TransformValueStat(it) { value -> value } },
            { FilterSeriesStat(it) { true } },
            { it.withWeight(1.0) },
            { it.weightBy { 1.0 } },
            { it.asDiscrete().asSeries() },
            { it.atX().withFixedY(0.0) },
            { it.atY().withFixedX(0.0) },
            { FoldPairedStat(it) { x, _ -> x }.withFixedY(0.0) },
            { it.mapResult({ result -> result }, { result -> result }) },
            { it.sample(1.0, 42L) },
        )
        for ((index, decorate) in decorators.withIndex()) {
            val stat = decorate(SumStat().diff()).windowed(60.seconds)
            repeat(6) { stat.update(it.toDouble(), it * 8_000_000_000L) }

            assertEquals(5.0, stat.read(40_000_000_000L).sum, DELTA, "decorator=$index")
        }
    }

    @Test
    fun `vector decorators preserve differences across window slices`() {
        val stat = TransformVectorStat(
            FilterVectorStat(SumStat().diff().atIndex(0)) { true },
            transform = { it },
        ).withWeight(1.0).weightBy { 1.0 }.sample(1.0, 42L).windowed(60.seconds)
        repeat(6) { stat.update(doubleArrayOf(it.toDouble()), it * 8_000_000_000L) }

        assertEquals(5.0, stat.read(40_000_000_000L).sum, DELTA)
    }

    @Test
    fun `vectorization preserves differences across window slices`() {
        val stat = VectorizedStat(2, SumStat().diff()).windowed(60.seconds)
        repeat(6) { stat.update(doubleArrayOf(it.toDouble(), 2.0 * it), it * 8_000_000_000L) }

        assertEquals(listOf(SumResult(5.0), SumResult(10.0)), stat.read(40_000_000_000L).results)
    }

    @Test
    fun `paired decorators preserve differences across window slices`() {
        val stat = TransformPairStat(
            FilterPairedStat(SumStat().diff().atX()) { _, _ -> true },
        ) { x, y -> x to y }.withWeight(1.0).weightBy { _, _ -> 1.0 }.sample(1.0, 42L).windowed(60.seconds)
        repeat(6) { stat.update(it.toDouble(), 0.0, it * 8_000_000_000L) }

        assertEquals(5.0, stat.read(40_000_000_000L).sum, DELTA)
    }

    @Test
    fun `discrete decorators preserve differences across window slices`() {
        val stat = TransformLongStat(
            FilterDiscreteStat(SumStat().diff().asDiscrete()) { true },
        ) { it }.withWeight(1.0).weightBy { 1.0 }.sample(1.0, 42L).windowed(60.seconds)
        repeat(6) { stat.update(it.toLong(), it * 8_000_000_000L) }

        assertEquals(5.0, stat.read(40_000_000_000L).sum, DELTA)
    }

    @Test
    fun `inert observations cannot rotate a live slice`() {
        for (weight in listOf(0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            val stat = sumWindowed()
            stat.update(5.0, T0)
            stat.update(99.0, T20, weight)

            assertEquals(5.0, stat.read(T0).sum, DELTA, "weight=$weight")
        }
    }
}
