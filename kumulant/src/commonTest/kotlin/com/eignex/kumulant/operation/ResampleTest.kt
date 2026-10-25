package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.spec.ResampleAggregator
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds

class ResampleTest {

    @Test
    fun `mean aggregator forwards one update per closed bucket`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        stat.update(value = 1.0, timestampNanos = 10_000_000L)
        stat.update(value = 3.0, timestampNanos = 50_000_000L)
        stat.update(value = 7.0, timestampNanos = 150_000_000L)
        assertEquals(2.0, stat.read().sum, DELTA)
    }

    @Test
    fun `sum aggregator forwards the bucket total`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Sum)
        stat.update(value = 1.0, timestampNanos = 10_000_000L)
        stat.update(value = 3.0, timestampNanos = 50_000_000L)
        stat.update(value = 7.0, timestampNanos = 150_000_000L)
        assertEquals(4.0, stat.read().sum, DELTA)
    }

    @Test
    fun `last aggregator forwards the last value in the bucket`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Last)
        stat.update(value = 1.0, timestampNanos = 10_000_000L)
        stat.update(value = 3.0, timestampNanos = 50_000_000L)
        stat.update(value = 9.9, timestampNanos = 80_000_000L)
        stat.update(value = 7.0, timestampNanos = 200_000_000L)
        assertEquals(9.9, stat.read().sum, DELTA)
    }

    @Test
    fun `min aggregator forwards the bucket minimum`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Min)
        stat.update(value = 5.0, timestampNanos = 10_000_000L)
        stat.update(value = 1.5, timestampNanos = 60_000_000L)
        stat.update(value = 3.0, timestampNanos = 80_000_000L)
        stat.update(value = 0.0, timestampNanos = 200_000_000L)
        assertEquals(1.5, stat.read().sum, DELTA)
    }

    @Test
    fun `max aggregator forwards the bucket maximum`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Max)
        stat.update(value = 5.0, timestampNanos = 10_000_000L)
        stat.update(value = 1.5, timestampNanos = 60_000_000L)
        stat.update(value = 9.0, timestampNanos = 80_000_000L)
        stat.update(value = 0.0, timestampNanos = 200_000_000L)
        assertEquals(9.0, stat.read().sum, DELTA)
    }

    @Test
    fun `in-progress bucket is held until a later bucket arrives`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        stat.update(value = 2.0, timestampNanos = 0L)
        stat.update(value = 4.0, timestampNanos = 50_000_000L)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `multiple closed buckets each forward one observation`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        stat.update(value = 1.0, timestampNanos = 0L)
        stat.update(value = 3.0, timestampNanos = 50_000_000L)
        stat.update(value = 4.0, timestampNanos = 110_000_000L)
        stat.update(value = 6.0, timestampNanos = 150_000_000L)
        stat.update(value = 7.0, timestampNanos = 250_000_000L)
        assertEquals(2.0 + 5.0, stat.read().sum, DELTA)
    }

    @Test
    fun `a downdate emptying the open bucket is rejected`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        stat.update(value = 10.0, timestampNanos = 10_000_000L, weight = 1.0)
        assertFailsWith<IllegalArgumentException> {
            stat.update(value = 20.0, timestampNanos = 50_000_000L, weight = -1.0)
        }
    }

    @Test
    fun `a downdate opening a bucket is rejected`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        assertFailsWith<IllegalArgumentException> {
            stat.update(value = 10.0, timestampNanos = 10_000_000L, weight = -1.0)
        }
    }

    @Test
    fun `a rejected downdate leaves the bucket as it was`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        stat.update(value = 10.0, timestampNanos = 10_000_000L, weight = 1.0)
        runCatching { stat.update(value = 20.0, timestampNanos = 50_000_000L, weight = -1.0) }
        stat.update(value = 7.0, timestampNanos = 150_000_000L, weight = 1.0)
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `a partial downdate is honoured as a weighted removal`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        stat.update(value = 10.0, timestampNanos = 10_000_000L, weight = 2.0)
        stat.update(value = 20.0, timestampNanos = 50_000_000L, weight = -1.0)
        stat.update(value = 7.0, timestampNanos = 150_000_000L, weight = 1.0)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `negative bucket duration is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            SumStat().resampleByTime(bucket = (-1L).milliseconds, aggregator = ResampleAggregator.Mean)
        }
    }

    @Test
    fun `reset clears the in-progress bucket and the delegate`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Sum)
        stat.update(value = 1.0, timestampNanos = 0L)
        stat.update(value = 7.0, timestampNanos = 200_000_000L)
        stat.reset()
        stat.update(value = 5.0, timestampNanos = 0L)
        stat.update(value = 9.0, timestampNanos = 250_000_000L)
        assertEquals(5.0, stat.read().sum, DELTA)
    }

    @Test
    fun `a late arrival joins the open bucket instead of closing it twice`() {
        val stat = SumStat().resampleByTime(bucket = 1000.milliseconds, aggregator = ResampleAggregator.Last)
        stat.update(value = 1.0, timestampNanos = 0L)
        stat.update(value = 2.0, timestampNanos = 2_000_000_000L)
        stat.update(value = 3.0, timestampNanos = 1_000_000_000L)
        stat.update(value = 4.0, timestampNanos = 2_500_000_000L)
        stat.update(value = 5.0, timestampNanos = 3_000_000_000L)
        assertEquals(5.0, stat.read().sum, DELTA)
    }
}

class ResampleDowndateGuardTest {

    @Test
    fun `a late downdate is charged against the bucket it lands in`() {
        val s = SumStat().resampleByTime(bucket = 1.microseconds, aggregator = ResampleAggregator.Mean)
        s.update(10.0, timestampNanos = 100L, weight = 1.0)
        s.update(20.0, timestampNanos = 1100L, weight = 3.0)
        s.update(20.0, timestampNanos = 500L, weight = -1.0)
        assertEquals(10.0, s.read(1200L).sum, DELTA)
    }
}
