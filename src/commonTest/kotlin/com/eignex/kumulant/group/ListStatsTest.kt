package com.eignex.kumulant.group

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stream.SerialMode

import com.eignex.kumulant.stream.StreamMode

import com.eignex.kumulant.stat.summary.Mean

import com.eignex.kumulant.stat.summary.Sum

import com.eignex.kumulant.stat.summary.Variance

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame

private const val DELTA = 1e-12

class ListStatsTest {

    @Test
    fun `update forwards to all child stats and read returns positional results`() {
        val stats = ListStats("a" to Sum(), "b" to Sum())

        stats.update(2.0, timestampNanos = 10L, weight = 3.0)
        stats.update(4.0, timestampNanos = 20L, weight = 0.5)

        val result = stats.read(30L)
        assertEquals(2, result.results.size)
        assertEquals(listOf("a", "b"), result.names)
        assertEquals(8.0, result.results[0].sum, DELTA)
        assertEquals(8.0, result.results[1].sum, DELTA)
    }

    @Test
    fun `auto-names entries by class simpleName`() {
        val stats = seriesListStats<Result>(Mean(), Sum())
        val map = stats.read().toMap()
        assertEquals(setOf("Mean", "Sum"), map.keys)
    }

    @Test
    fun `duplicate auto-names throw`() {
        val ex = assertFailsWith<IllegalArgumentException> {
            seriesListStats<SumResult>(Sum(), Sum())
        }
        check("Duplicate" in (ex.message ?: "")) { "got: ${ex.message}" }
    }

    @Test
    fun `explicit names disambiguate same-type stats`() {
        val stats = ListStats("in" to Mean(), "out" to Mean())
        stats.update(5.0)
        val map = stats.read().toMap()
        assertEquals(setOf("in", "out"), map.keys)
    }

    @Test
    fun `merge combines by position`() {
        val target = ListStats("a" to Sum(), "b" to Sum())
        target.update(10.0)

        val source = ListStats("a" to Sum(), "b" to Sum())
        source.update(2.0)
        source.update(3.0)

        target.merge(source.read())

        val merged = target.read()
        assertEquals(15.0, merged.results[0].sum, DELTA)
        assertEquals(15.0, merged.results[1].sum, DELTA)
    }

    @Test
    fun `reset clears all child stats`() {
        val stats = ListStats("a" to Sum(), "b" to Sum())
        stats.update(5.0)
        stats.update(1.0)

        stats.reset()

        val result = stats.read()
        assertEquals(0.0, result.results[0].sum, DELTA)
        assertEquals(0.0, result.results[1].sum, DELTA)
    }

    @Test
    fun `create returns independent list`() {
        val original = ListStats("a" to Sum(), "b" to Sum())
        original.update(2.0)

        val created = original.create()
        created.update(3.0)

        val originalResult = original.read()
        val createdResult = created.read()
        assertEquals(2.0, originalResult.results[0].sum, DELTA)
        assertEquals(2.0, originalResult.results[1].sum, DELTA)
        assertEquals(3.0, createdResult.results[0].sum, DELTA)
        assertEquals(3.0, createdResult.results[1].sum, DELTA)
    }

    @Test
    fun `create uses list mode when create mode is null`() {
        var childCreateMode: StreamMode? = null

        val tracking = object : SeriesStat<SumResult> {
            override val mode: StreamMode = SerialMode
            override fun update(value: Double, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: SumResult) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = SumResult(0.0)
            override fun create(mode: StreamMode?): SeriesStat<SumResult> {
                childCreateMode = mode
                return this
            }
        }

        val stats = ListStats(listOf("t" to tracking), mode = SerialMode)
        stats.create(null)

        assertSame(SerialMode, childCreateMode)
    }

    @Test
    fun `nested ListStats reflects nesting in toMap`() {
        val stats = ListStats<Result>(
            "top" to Mean(),
            "nested" to seriesListStats<Result>(Mean(), Variance()),
        )
        stats.update(2.0)
        stats.update(4.0)

        val map = stats.read().toMap()
        assertEquals(setOf("top", "nested"), map.keys)

        val nested = assertIs<ResultList<*>>(map["nested"])
        val nestedMap = nested.toMap()
        assertEquals(setOf("Mean", "Variance"), nestedMap.keys)
    }

    @Test
    fun `positional ResultList constructor uses index names`() {
        val rl = ResultList(listOf(SumResult(1.0), SumResult(2.0)))
        assertEquals(listOf("0", "1"), rl.names)
        assertEquals(SumResult(1.0), rl.toMap()["0"])
    }
}
