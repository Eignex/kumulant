package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds

private const val DELTA = 1e-12

class ResampleTest {

    @Test
    fun `mean aggregator forwards one update per closed bucket`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        // Bucket 0: values 1.0, 3.0 -> mean 2.0. Bucket 1: value 7.0 (in-progress, not yet flushed).
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
        stat.update(value = 7.0, timestampNanos = 150_000_000L) // triggers flush of bucket 0
        assertEquals(4.0, stat.read().sum, DELTA)
    }

    @Test
    fun `last aggregator forwards the last value in the bucket`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Last)
        stat.update(value = 1.0, timestampNanos = 10_000_000L)
        stat.update(value = 3.0, timestampNanos = 50_000_000L)
        stat.update(value = 9.9, timestampNanos = 80_000_000L)
        stat.update(value = 7.0, timestampNanos = 200_000_000L) // closes bucket 0 (last=9.9)
        assertEquals(9.9, stat.read().sum, DELTA)
    }

    @Test
    fun `min aggregator forwards the bucket minimum`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Min)
        stat.update(value = 5.0, timestampNanos = 10_000_000L)
        stat.update(value = 1.5, timestampNanos = 60_000_000L)
        stat.update(value = 3.0, timestampNanos = 80_000_000L)
        stat.update(value = 0.0, timestampNanos = 200_000_000L) // closes bucket 0 -> min=1.5
        assertEquals(1.5, stat.read().sum, DELTA)
    }

    @Test
    fun `max aggregator forwards the bucket maximum`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Max)
        stat.update(value = 5.0, timestampNanos = 10_000_000L)
        stat.update(value = 1.5, timestampNanos = 60_000_000L)
        stat.update(value = 9.0, timestampNanos = 80_000_000L)
        stat.update(value = 0.0, timestampNanos = 200_000_000L) // closes bucket 0 -> max=9.0
        assertEquals(9.0, stat.read().sum, DELTA)
    }

    @Test
    fun `in-progress bucket is held until a later bucket arrives`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        stat.update(value = 2.0, timestampNanos = 0L)
        stat.update(value = 4.0, timestampNanos = 50_000_000L)
        // No update in a later bucket yet, so nothing has flushed.
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `multiple closed buckets each forward one observation`() {
        val stat = SumStat().resampleByTime(bucket = 100.milliseconds, aggregator = ResampleAggregator.Mean)
        // Bucket 0: mean 2.0
        stat.update(value = 1.0, timestampNanos = 0L)
        stat.update(value = 3.0, timestampNanos = 50_000_000L)
        // Bucket 1: mean 5.0 (closes bucket 0)
        stat.update(value = 4.0, timestampNanos = 110_000_000L)
        stat.update(value = 6.0, timestampNanos = 150_000_000L)
        // Bucket 2: mean 7.0 (closes bucket 1)
        stat.update(value = 7.0, timestampNanos = 250_000_000L)
        assertEquals(2.0 + 5.0, stat.read().sum, DELTA)
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
        stat.update(value = 7.0, timestampNanos = 200_000_000L) // forwards bucket 0 -> 1.0
        stat.reset()
        stat.update(value = 5.0, timestampNanos = 0L)
        stat.update(value = 9.0, timestampNanos = 250_000_000L) // forwards bucket 0 -> 5.0
        assertEquals(5.0, stat.read().sum, DELTA)
    }
}
