package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

private const val DELTA = 1e-12

class ListStatsTest {

    @Test
    fun `update forwards to all child stats and read returns positional results`() {
        val stats = ListStats<SumResult>(
            Sum(),
            Sum().withValue(1.0).withWeight(1.0)
        )

        stats.update(2.0, timestampNanos = 10L, weight = 3.0)
        stats.update(4.0, timestampNanos = 20L, weight = 0.5)

        val result = stats.read(30L)
        assertEquals(2, result.results.size)
        assertEquals(8.0, result.results[0].sum, DELTA)
        assertEquals(2.0, result.results[1].sum, DELTA)
    }

    @Test
    fun `merge combines by position`() {
        val target = ListStats<SumResult>(Sum(), Sum().withValue(1.0).withWeight(1.0))
        target.update(10.0)

        val source = ListStats<SumResult>(Sum(), Sum().withValue(1.0).withWeight(1.0))
        source.update(2.0)
        source.update(3.0)

        target.merge(source.read())

        val merged = target.read()
        assertEquals(15.0, merged.results[0].sum, DELTA)
        assertEquals(3.0, merged.results[1].sum, DELTA)
    }

    @Test
    fun `reset clears all child stats`() {
        val stats = ListStats<SumResult>(Sum(), Sum().withValue(1.0).withWeight(1.0))
        stats.update(5.0)
        stats.update(1.0)

        stats.reset()

        val result = stats.read()
        assertEquals(0.0, result.results[0].sum, DELTA)
        assertEquals(0.0, result.results[1].sum, DELTA)
    }

    @Test
    fun `create returns independent list`() {
        val original = ListStats<SumResult>(Sum(), Sum().withValue(1.0).withWeight(1.0))
        original.update(2.0)

        val created = original.create()
        created.update(3.0)

        val originalResult = original.read()
        val createdResult = created.read()
        assertEquals(2.0, originalResult.results[0].sum, DELTA)
        assertEquals(1.0, originalResult.results[1].sum, DELTA)
        assertEquals(3.0, createdResult.results[0].sum, DELTA)
        assertEquals(1.0, createdResult.results[1].sum, DELTA)
    }

    @Test
    fun `create uses list mode when create mode is null`() {
        var childCreateMode: StreamMode? = null

        val tracking = object : SeriesStat<SumResult> {
            override fun update(value: Double, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: SumResult) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = SumResult(0.0)
            override fun create(mode: StreamMode?): SeriesStat<SumResult> {
                childCreateMode = mode
                return this
            }
        }

        val stats = ListStats(listOf(tracking), mode = SerialMode)
        stats.create(null)

        assertSame(SerialMode, childCreateMode)
    }
}
