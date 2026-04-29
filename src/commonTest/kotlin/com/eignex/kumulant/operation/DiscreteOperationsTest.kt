package com.eignex.kumulant.operation

import com.eignex.kumulant.core.HyperLogLogResult
import com.eignex.kumulant.core.LinearCountingResult
import com.eignex.kumulant.stat.HyperLogLogPlus
import com.eignex.kumulant.stat.LinearCounting
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FilterDiscreteStatTest {
    @Test
    fun `predicate gates updates`() {
        val stat = HyperLogLogPlus(precision = 10).filter { it >= 0L }
        for (i in -50L..50L) stat.update(i)
        val seen = stat.read().estimate
        // Only 51 keys (0..50) should reach the underlying HLL.
        assertTrue(seen in 45.0..56.0, "estimate=$seen")
    }

    @Test
    fun `create preserves predicate`() {
        val template = LinearCounting(bits = 1024).filter { it % 2L == 0L }
        val fresh = template.create()
        for (i in 1L..100L) fresh.update(i)
        val seen = fresh.read().estimate
        // Only the 50 even values are admitted.
        assertTrue(seen in 40.0..60.0, "estimate=$seen")
    }
}

class TransformDiscreteStatTest {
    @Test
    fun `transformValue is applied before update`() {
        val stat = HyperLogLogPlus(precision = 10).transformValue { it / 10L }
        for (i in 0L..99L) stat.update(i)
        val seen = stat.read().estimate
        // 100 inputs collapse to 10 distinct buckets after the transform.
        assertTrue(seen in 8.0..12.0, "estimate=$seen")
    }

    @Test
    fun `withValue replaces input with constant`() {
        val stat = LinearCounting(bits = 1024).withValue(7L)
        for (i in 1L..100L) stat.update(i)
        val seen = stat.read().estimate
        assertTrue(seen in 0.5..2.0, "estimate=$seen")
    }
}

class WithWeightDiscreteStatTest {
    @Test
    fun `zero-weight wrapper drops all updates`() {
        val stat = HyperLogLogPlus(precision = 10).withWeight(0.0)
        for (i in 1L..100L) stat.update(i, weight = 1.0)
        assertEquals(0.0, stat.read().estimate)
    }

    @Test
    fun `positive weight wrapper passes through`() {
        val stat = LinearCounting(bits = 1024).withWeight(1.0)
        for (i in 1L..50L) stat.update(i, weight = 0.0) // caller weight ignored
        val seen = stat.read().estimate
        assertTrue(seen in 40.0..60.0, "estimate=$seen")
    }
}

class MapResultDiscreteStatTest {
    @Test
    fun `forward transform applied on read`() {
        val forward: (HyperLogLogResult) -> LinearCountingResult = {
            LinearCountingResult(
                estimate = it.estimate,
                bits = 0,
                unsetBits = 0L,
                words = LongArray(0),
                totalSeen = it.totalSeen,
            )
        }
        val reverse: (LinearCountingResult) -> HyperLogLogResult = {
            HyperLogLogResult(
                estimate = it.estimate,
                precision = 10,
                registers = IntArray(1024),
                totalSeen = it.totalSeen,
            )
        }
        val stat = HyperLogLogPlus(precision = 10).mapResult(forward, reverse)
        for (i in 1L..100L) stat.update(i)
        val seen = stat.read().estimate
        assertTrue(seen in 80.0..120.0, "estimate=$seen")
    }
}
