package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.operation.VectorizedStat
import com.eignex.kumulant.stat.Mean
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame

private const val DELTA = 1e-9

private fun sumVector(d: Int) = VectorizedStat<SumResult>(d, template = { Sum() })
private fun meanVector(d: Int) = VectorizedStat<WeightedMeanResult>(d, template = { Mean() })

class VectorStatGroupTest {

    @Test
    fun `update forwards vectors to all child stats`() {
        val sumKey = StatKey<ResultList<SumResult>>("sums") to sumVector(2)
        val meanKey = StatKey<ResultList<WeightedMeanResult>>("means") to meanVector(2)

        val group = VectorStatGroup(sumKey, meanKey)
        group.update(doubleArrayOf(1.0, 10.0))
        group.update(doubleArrayOf(3.0, 20.0))

        val result = group.read()
        val sums = result[sumKey.first]
        assertEquals(4.0, sums.results[0].sum, DELTA)
        assertEquals(30.0, sums.results[1].sum, DELTA)

        val means = result[meanKey.first]
        assertEquals(2.0, means.results[0].mean, DELTA)
        assertEquals(15.0, means.results[1].mean, DELTA)
    }

    @Test
    fun `merge delegates only to keys present in incoming result`() {
        val sumKey = StatKey<ResultList<SumResult>>("sums") to sumVector(2)
        val meanKey = StatKey<ResultList<WeightedMeanResult>>("means") to meanVector(2)

        val target = VectorStatGroup(sumKey, meanKey)
        target.update(doubleArrayOf(1.0, 10.0))

        val source = VectorStatGroup(
            StatKey<ResultList<SumResult>>("sums") to sumVector(2),
            StatKey<ResultList<WeightedMeanResult>>("means") to meanVector(2)
        )
        source.update(doubleArrayOf(2.0, 20.0))
        source.update(doubleArrayOf(3.0, 30.0))

        val onlySums = GroupResult(
            results = mapOf("sums" to source.read()[StatKey<ResultList<SumResult>>("sums")])
        )

        target.merge(onlySums)
        val merged = target.read()
        assertEquals(6.0, merged[sumKey.first].results[0].sum, DELTA)
        assertEquals(60.0, merged[sumKey.first].results[1].sum, DELTA)
        // means was not in merge payload, still reflects the single initial update
        assertEquals(1.0, merged[meanKey.first].results[0].totalWeights, DELTA)
    }

    @Test
    fun `reset clears all child stats`() {
        val sumKey = StatKey<ResultList<SumResult>>("sums") to sumVector(2)
        val group = VectorStatGroup(sumKey)
        group.update(doubleArrayOf(1.0, 10.0))
        group.reset()
        assertEquals(0.0, group.read()[sumKey.first].results[0].sum, DELTA)
    }

    @Test
    fun `create returns an independent group`() {
        val sumKey = StatKey<ResultList<SumResult>>("sums") to sumVector(2)
        val original = VectorStatGroup(sumKey)
        original.update(doubleArrayOf(1.0, 2.0))
        val clone = original.create()
        clone.update(doubleArrayOf(5.0, 6.0))

        assertEquals(1.0, original.read()[sumKey.first].results[0].sum, DELTA)
        assertEquals(5.0, clone.read()[sumKey.first].results[0].sum, DELTA)
    }

    @Test
    fun `create uses group mode when create mode is null`() {
        var childCreateMode: StreamMode? = null

        val tracking = object : VectorStat<ResultList<SumResult>> {
            override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: ResultList<SumResult>) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = ResultList<SumResult>(emptyList())
            override fun create(mode: StreamMode?): VectorStat<ResultList<SumResult>> {
                childCreateMode = mode
                return this
            }
        }

        val group = VectorStatGroup(
            StatKey<ResultList<SumResult>>("v") to tracking,
            mode = SerialMode
        )
        group.create(null)
        assertSame(SerialMode, childCreateMode)
    }
}

class VectorListStatsTest {

    @Test
    fun `update forwards to all child stats`() {
        val stats = VectorListStats<Result>(
            "sums" to sumVector(2),
            "means" to meanVector(2),
        )
        stats.update(doubleArrayOf(1.0, 10.0))
        stats.update(doubleArrayOf(3.0, 20.0))

        val r = stats.read()
        assertEquals(listOf("sums", "means"), r.names)
        val sums = assertIs<ResultList<*>>(r.results[0])
        val firstSum = assertIs<SumResult>(sums.results[0])
        assertEquals(4.0, firstSum.sum, DELTA)
    }

    @Test
    fun `duplicate names throw at construction`() {
        assertFailsWith<IllegalArgumentException> {
            VectorListStats<ResultList<SumResult>>(
                "a" to sumVector(2),
                "a" to sumVector(2)
            )
        }
    }

    @Test
    fun `vectorListStats factory auto-names single stat by simpleName`() {
        val stats = vectorListStats<ResultList<SumResult>>(sumVector(2))
        assertEquals(setOf("VectorizedStat"), stats.read().toMap().keys)
    }

    @Test
    fun `vectorListStats factory rejects duplicate auto-names`() {
        assertFailsWith<IllegalArgumentException> {
            vectorListStats<ResultList<SumResult>>(sumVector(2), sumVector(2))
        }
    }

    @Test
    fun `reset clears all child stats`() {
        val stats = VectorListStats<ResultList<SumResult>>("s" to sumVector(2))
        stats.update(doubleArrayOf(1.0, 2.0))
        stats.reset()
        val inner = assertIs<ResultList<*>>(stats.read().results[0])
        val firstSum = assertIs<SumResult>(inner.results[0])
        assertEquals(0.0, firstSum.sum, DELTA)
    }

    @Test
    fun `create returns independent list`() {
        val original = VectorListStats<ResultList<SumResult>>("s" to sumVector(2))
        original.update(doubleArrayOf(1.0, 2.0))
        val clone = original.create()
        clone.update(doubleArrayOf(5.0, 6.0))

        val origInner = assertIs<ResultList<*>>(original.read().results[0])
        val cloneInner = assertIs<ResultList<*>>(clone.read().results[0])
        val origFirst = assertIs<SumResult>(origInner.results[0])
        val cloneFirst = assertIs<SumResult>(cloneInner.results[0])
        assertEquals(1.0, origFirst.sum, DELTA)
        assertEquals(5.0, cloneFirst.sum, DELTA)
    }

    @Test
    fun `merge combines each position`() {
        val target = VectorListStats<ResultList<SumResult>>("s" to sumVector(2))
        target.update(doubleArrayOf(1.0, 2.0))

        val source = VectorListStats<ResultList<SumResult>>("s" to sumVector(2))
        source.update(doubleArrayOf(3.0, 4.0))

        target.merge(source.read())
        val inner = assertIs<ResultList<*>>(target.read().results[0])
        val firstSum = assertIs<SumResult>(inner.results[0])
        assertEquals(4.0, firstSum.sum, DELTA)
    }

    @Test
    fun `create uses list mode when create mode is null`() {
        var childCreateMode: StreamMode? = null

        val tracking = object : VectorStat<ResultList<SumResult>> {
            override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: ResultList<SumResult>) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = ResultList<SumResult>(emptyList())
            override fun create(mode: StreamMode?): VectorStat<ResultList<SumResult>> {
                childCreateMode = mode
                return this
            }
        }

        val stats = VectorListStats(listOf("t" to tracking), mode = SerialMode)
        stats.create(null)
        assertSame(SerialMode, childCreateMode)
    }
}
