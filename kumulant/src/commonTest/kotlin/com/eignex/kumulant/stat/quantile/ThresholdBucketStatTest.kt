package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.assertModesAgree
import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ThresholdBucketStatTest {

    @Test
    fun `single threshold partitions inputs into two buckets`() {
        val s = ThresholdBucketStat(doubleArrayOf(5.0))
        listOf(1.0, 5.0, 6.0, 100.0, 0.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(listOf(3.0, 2.0), r.counts)
    }

    @Test
    fun `multiple thresholds count per bucket`() {
        val s = ThresholdBucketStat(doubleArrayOf(0.0, 10.0, 100.0))
        listOf(-5.0, 0.0, 5.0, 10.0, 50.0, 100.0, 1000.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(listOf(2.0, 2.0, 2.0, 1.0), r.counts)
    }

    @Test
    fun `weight contributes to the resolved bucket`() {
        val s = ThresholdBucketStat(doubleArrayOf(0.0))
        s.update(value = 1.0, weight = 2.5)
        s.update(value = -1.0, weight = 1.5)
        val r = s.read()
        assertEquals(listOf(1.5, 2.5), r.counts)
    }

    @Test
    fun `merge sums bucket counts`() {
        val a = ThresholdBucketStat(doubleArrayOf(0.0)).apply {
            update(1.0)
            update(2.0)
            update(-3.0)
        }
        val b = ThresholdBucketStat(doubleArrayOf(0.0)).apply {
            update(-1.0)
            update(5.0)
        }
        a.merge(b.read())
        assertEquals(listOf(2.0, 3.0), a.read().counts)
    }

    @Test
    fun `reset clears all buckets`() {
        val s = ThresholdBucketStat(doubleArrayOf(0.0, 10.0))
        listOf(-1.0, 5.0, 50.0).forEach { s.update(it) }
        s.reset()
        assertEquals(listOf(0.0, 0.0, 0.0), s.read().counts)
    }

    @Test
    fun `non-increasing thresholds are rejected`() {
        assertFailsWith<IllegalArgumentException> {
            ThresholdBucketStat(doubleArrayOf(1.0, 1.0))
        }
        assertFailsWith<IllegalArgumentException> {
            ThresholdBucketStat(doubleArrayOf(5.0, 3.0))
        }
    }

    @Test
    fun `empty thresholds are rejected`() {
        assertFailsWith<IllegalArgumentException> {
            ThresholdBucketStat(doubleArrayOf())
        }
    }

    @Test
    fun `result carries the configured thresholds`() {
        val thresholds = listOf(0.0, 10.0, 100.0)
        val s = ThresholdBucketStat(thresholds.toDoubleArray())
        s.update(5.0)
        assertEquals(thresholds, s.read().thresholds)
    }

    @Test
    fun `read on a fresh stat returns zero counts`() {
        val r = ThresholdBucketStat(doubleArrayOf(0.0, 1.0)).read()
        assertEquals(listOf(0.0, 0.0, 0.0), r.counts)
        for (c in r.counts) assertEquals(0.0, c, DELTA)
    }

    @Test
    fun `sequential math equal across concurrency modes`() {
        val values = doubleArrayOf(1.0, -2.0, 3.5, 0.0, 4.2, -1.1, 7.0, 2.5)
        val reads = Concurrency.entries.associateWith { mode ->
            val s = ThresholdBucketStat(doubleArrayOf(-1.0, 0.0, 1.0, 3.0), concurrency = mode)
            for (v in values) s.update(v)
            s.read()
        }
        assertModesAgree("ThresholdBucketStat", reads)
    }
}
