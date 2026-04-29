package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.operation.VectorizedStat
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight
import com.eignex.kumulant.stat.Covariance
import com.eignex.kumulant.stat.HyperLogLogPlus
import com.eignex.kumulant.stat.LinearCounting
import com.eignex.kumulant.stat.Mean
import com.eignex.kumulant.stat.OLS
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

private const val DELTA = 1e-12

private fun sumVector(d: Int) = VectorizedStat<SumResult>(d, template = { Sum() })
private fun meanVector(d: Int) = VectorizedStat<WeightedMeanResult>(d, template = { Mean() })

class StatGroupTest {

    @Test
    fun `update forwards values to all child stats and read returns grouped result`() {
        val sum = StatKey<SumResult>("sum") to Sum()
        val count = StatKey<SumResult>("count") to Sum().withValue(1.0).withWeight(1.0)

        val group = StatGroup(sum, count)

        group.update(2.0, timestampNanos = 10L, weight = 3.0)
        group.update(4.0, timestampNanos = 20L, weight = 0.5)

        val result = group.read(30L)
        assertEquals(2, result.results.size)
        assertEquals(8.0, result[sum.first].sum, DELTA)
        assertEquals(2.0, result[count.first].sum, DELTA)
        assertTrue(result.results.containsKey("sum"))
        assertTrue(result.results.containsKey("count"))
    }

    @Test
    fun `merge merges only keys present in incoming grouped result`() {
        val sum = StatKey<SumResult>("sum") to Sum()
        val count = StatKey<SumResult>("count") to Sum().withValue(1.0).withWeight(1.0)

        val target = StatGroup(sum, count)
        target.update(10.0)

        val source = StatGroup(
            StatKey<SumResult>("sum") to Sum(),
            StatKey<SumResult>("count") to Sum().withValue(1.0).withWeight(1.0)
        )
        source.update(2.0)
        source.update(3.0)

        val incoming = GroupResult(results = mapOf("sum" to source.read()[StatKey("sum")]))

        target.merge(incoming)
        val merged = target.read()

        assertEquals(15.0, merged[sum.first].sum, DELTA)
        assertEquals(1.0, merged[count.first].sum, DELTA)
    }

    @Test
    fun `reset resets all child stats`() {
        val sum = StatKey<SumResult>("sum") to Sum()
        val count = StatKey<SumResult>("count") to Sum().withValue(1.0).withWeight(1.0)

        val group = StatGroup(sum, count)
        group.update(5.0)
        group.update(1.0)

        group.reset()

        val result = group.read()
        assertEquals(0.0, result[sum.first].sum, DELTA)
        assertEquals(0.0, result[count.first].sum, DELTA)
    }

    @Test
    fun `create returns independent group`() {
        val sum = StatKey<SumResult>("sum") to Sum()
        val count = StatKey<SumResult>("count") to Sum().withValue(1.0).withWeight(1.0)

        val original = StatGroup(sum, count)
        original.update(2.0)

        val created = original.create()
        created.update(3.0)

        val originalResult = original.read()
        val createdResult = created.read()
        assertEquals(2.0, originalResult[sum.first].sum, DELTA)
        assertEquals(1.0, originalResult[count.first].sum, DELTA)
        assertEquals(3.0, createdResult[sum.first].sum, DELTA)
        assertEquals(1.0, createdResult[count.first].sum, DELTA)
    }

    @Test
    fun `supports hierarchical stat groups`() {
        val nestedSum = StatKey<SumResult>("leafSum") to Sum()
        val nestedCount = StatKey<SumResult>("leafCount") to Sum().withValue(1.0).withWeight(1.0)
        val nested = StatKey<GroupResult>("nested") to StatGroup(nestedSum, nestedCount)
        val topSum = StatKey<SumResult>("topSum") to Sum()

        val target = StatGroup(nested, topSum)

        target.update(2.0, timestampNanos = 10L, weight = 2.0)
        target.update(3.0, timestampNanos = 20L, weight = 1.0)

        val read = target.read(30L)
        assertEquals(7.0, read[topSum.first].sum, DELTA)
        assertEquals(7.0, read[nested.first][nestedSum.first].sum, DELTA)
        assertEquals(2.0, read[nested.first][nestedCount.first].sum, DELTA)

        val source = StatGroup(
            StatKey<GroupResult>("nested") to StatGroup(
                StatKey<SumResult>("leafSum") to Sum(),
                StatKey<SumResult>("leafCount") to Sum().withValue(1.0).withWeight(1.0)
            ),
            StatKey<SumResult>("topSum") to Sum()
        )
        source.update(5.0)

        target.merge(source.read())
        val merged = target.read()
        assertEquals(12.0, merged[topSum.first].sum, DELTA)
        assertEquals(12.0, merged[nested.first][nestedSum.first].sum, DELTA)
        assertEquals(3.0, merged[nested.first][nestedCount.first].sum, DELTA)
    }

    @Test
    fun `nested keys remain independent while avoiding top-level conflicts`() {
        val topSum = StatKey<SumResult>("sum") to Sum()
        val nestedSum = StatKey<SumResult>("sum") to Sum()
        val nested = StatKey<GroupResult>("nested") to StatGroup(nestedSum)

        val group = StatGroup(topSum, nested)

        group.update(4.0)
        val read = group.read()

        assertEquals(4.0, read[topSum.first].sum, DELTA)
        assertEquals(4.0, read[nested.first][nestedSum.first].sum, DELTA)
        assertTrue(read.results.containsKey("sum"))
        assertTrue(read[nested.first].results.containsKey("sum"))
    }

    @Test
    fun `hierarchical composition can declare keys and stats together`() {
        val httpCount = StatKey<SumResult>("count") to Sum().withValue(1.0).withWeight(1.0)
        val httpTotalMs = StatKey<SumResult>("totalMs") to Sum()
        val http = StatKey<GroupResult>("http") to StatGroup(httpCount, httpTotalMs)

        val dbCount = StatKey<SumResult>("count") to Sum().withValue(1.0).withWeight(1.0)
        val dbTotalMs = StatKey<SumResult>("totalMs") to Sum()
        val db = StatKey<GroupResult>("db") to StatGroup(dbCount, dbTotalMs)

        val requests = StatKey<SumResult>("requests") to Sum().withValue(1.0).withWeight(1.0)

        val service = StatGroup(http, db, requests)

        service.update(120.0)
        service.update(80.0)

        val read = service.read()
        assertEquals(2.0, read[requests.first].sum, DELTA)
        assertEquals(2.0, read[http.first][httpCount.first].sum, DELTA)
        assertEquals(200.0, read[http.first][httpTotalMs.first].sum, DELTA)
        assertEquals(2.0, read[db.first][dbCount.first].sum, DELTA)
        assertEquals(200.0, read[db.first][dbTotalMs.first].sum, DELTA)
    }

    @Test
    fun `create uses group mode when create mode is null`() {
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

        val group = StatGroup(
            StatKey<SumResult>("sum") to tracking,
            mode = SerialMode
        )

        group.create(null)

        assertSame(SerialMode, childCreateMode)
    }

    @Test
    fun `stat schema helper supports namespaced composition and lifecycle operations`() {
        class HttpMetrics : StatSchema() {
            val requests by series(Sum().withValue(1.0).withWeight(1.0))
            val latencyMsTotal by series(Sum())
        }

        class DbMetrics : StatSchema() {
            val requests by series(Sum().withValue(1.0).withWeight(1.0))
            val latencyMsTotal by series(Sum())
        }

        class ServiceMetrics : StatSchema() {
            val requests by series(Sum().withValue(1.0).withWeight(1.0))
            val billableMsTotal by series(Sum())
            val http by group(HttpMetrics())
            val db by group(DbMetrics())
        }

        val schema = ServiceMetrics()
        val service = StatGroup(schema)

        service.update(120.0)
        service.update(80.0)

        val firstRead = service.read()
        assertEquals(2.0, firstRead[schema.requests].sum, DELTA)
        assertEquals(200.0, firstRead[schema.billableMsTotal].sum, DELTA)
        assertEquals(2.0, firstRead[schema.http, { requests }].sum, DELTA)
        assertEquals(200.0, firstRead[schema.http, { latencyMsTotal }].sum, DELTA)
        assertEquals(2.0, firstRead[schema.db, { requests }].sum, DELTA)
        assertEquals(200.0, firstRead[schema.db, { latencyMsTotal }].sum, DELTA)
        assertEquals(setOf("requests", "billableMsTotal", "http", "db"), firstRead.results.keys)
        assertTrue(firstRead.results["http"] is GroupResult)
        assertTrue(firstRead.results["db"] is GroupResult)

        val incoming = StatGroup(ServiceMetrics())
        incoming.update(50.0)
        incoming.update(30.0)
        service.merge(incoming.read())

        val merged = service.read()
        assertEquals(4.0, merged[schema.requests].sum, DELTA)
        assertEquals(280.0, merged[schema.billableMsTotal].sum, DELTA)
        assertEquals(4.0, merged[schema.http, { requests }].sum, DELTA)
        assertEquals(280.0, merged[schema.http, { latencyMsTotal }].sum, DELTA)
        assertEquals(4.0, merged[schema.db, { requests }].sum, DELTA)
        assertEquals(280.0, merged[schema.db, { latencyMsTotal }].sum, DELTA)

        val cloned = service.create()
        val clonedBeforeUpdate = cloned.read()
        assertEquals(0.0, clonedBeforeUpdate[schema.requests].sum, DELTA)
        assertEquals(0.0, clonedBeforeUpdate[schema.billableMsTotal].sum, DELTA)
        assertEquals(0.0, clonedBeforeUpdate[schema.http, { requests }].sum, DELTA)
        assertEquals(0.0, clonedBeforeUpdate[schema.http, { latencyMsTotal }].sum, DELTA)

        cloned.update(10.0)

        assertEquals(4.0, service.read()[schema.requests].sum, DELTA)
        assertEquals(1.0, cloned.read()[schema.requests].sum, DELTA)
        assertEquals(10.0, cloned.read()[schema.billableMsTotal].sum, DELTA)
        assertEquals(1.0, cloned.read()[schema.http, { requests }].sum, DELTA)
        assertEquals(10.0, cloned.read()[schema.http, { latencyMsTotal }].sum, DELTA)
        assertEquals(1.0, cloned.read()[schema.db, { requests }].sum, DELTA)
        assertEquals(10.0, cloned.read()[schema.db, { latencyMsTotal }].sum, DELTA)

        service.reset()
        val reset = service.read()
        assertEquals(0.0, reset[schema.requests].sum, DELTA)
        assertEquals(0.0, reset[schema.billableMsTotal].sum, DELTA)
        assertEquals(0.0, reset[schema.http, { requests }].sum, DELTA)
        assertEquals(0.0, reset[schema.http, { latencyMsTotal }].sum, DELTA)
        assertEquals(0.0, reset[schema.db, { requests }].sum, DELTA)
        assertEquals(0.0, reset[schema.db, { latencyMsTotal }].sum, DELTA)

        val clonedAfterReset = cloned.read()
        assertEquals(1.0, clonedAfterReset[schema.requests].sum, DELTA)
        assertEquals(10.0, clonedAfterReset[schema.billableMsTotal].sum, DELTA)
    }
}

class PairedStatGroupTest {

    @Test
    fun `update forwards x and y pairs to all child stats`() {
        val ols = StatKey<OLSResult>("ols") to OLS()
        val cov = StatKey<CovarianceResult>("cov") to Covariance()

        val group = PairedStatGroup(ols, cov)
        group.update(1.0, 2.0)
        group.update(2.0, 4.0)
        group.update(3.0, 6.0)

        val result = group.read()
        assertEquals(2, result.results.size)
        val olsResult = result[ols.first]
        assertEquals(2.0, olsResult.slope, DELTA)
        assertEquals(0.0, olsResult.intercept, DELTA)

        val covResult = result[cov.first]
        assertEquals(3.0, covResult.totalWeights, DELTA)
    }

    @Test
    fun `merge delegates only to keys present in incoming result`() {
        val olsKey = StatKey<OLSResult>("ols") to OLS()
        val covKey = StatKey<CovarianceResult>("cov") to Covariance()

        val target = PairedStatGroup(olsKey, covKey)
        target.update(1.0, 2.0)

        val source = PairedStatGroup(
            StatKey<OLSResult>("ols") to OLS(),
            StatKey<CovarianceResult>("cov") to Covariance()
        )
        source.update(2.0, 4.0)
        source.update(3.0, 6.0)

        val onlyOls = GroupResult(results = mapOf("ols" to source.read()[StatKey<OLSResult>("ols")]))

        target.merge(onlyOls)
        val merged = target.read()
        assertEquals(3.0, merged[olsKey.first].totalWeights, DELTA)

        assertEquals(1.0, merged[covKey.first].totalWeights, DELTA)
    }

    @Test
    fun `reset clears all child stats`() {
        val olsKey = StatKey<OLSResult>("ols") to OLS()
        val group = PairedStatGroup(olsKey)
        group.update(1.0, 2.0)
        group.update(2.0, 4.0)
        group.reset()
        assertEquals(0.0, group.read()[olsKey.first].totalWeights, DELTA)
    }

    @Test
    fun `create returns an independent group`() {
        val olsKey = StatKey<OLSResult>("ols") to OLS()
        val original = PairedStatGroup(olsKey)
        original.update(1.0, 2.0)

        val clone = original.create()
        clone.update(3.0, 6.0)

        assertEquals(1.0, original.read()[olsKey.first].totalWeights, DELTA)
        assertEquals(1.0, clone.read()[olsKey.first].totalWeights, DELTA)
    }

    @Test
    fun `create uses group mode when create mode is null`() {
        var childCreateMode: StreamMode? = null

        val tracking = object : PairedStat<OLSResult> {
            override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: OLSResult) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) =
                OLSResult(0.0, 0.0, 0.0, 0.0, VarianceResult(0.0, 0.0), VarianceResult(0.0, 0.0))
            override fun create(mode: StreamMode?): PairedStat<OLSResult> {
                childCreateMode = mode
                return this
            }
        }

        val group = PairedStatGroup(
            StatKey<OLSResult>("ols") to tracking,
            mode = SerialMode
        )
        group.create(null)
        assertSame(SerialMode, childCreateMode)
    }
}

class PairedListStatsTest {

    @Test
    fun `update forwards to all child stats`() {
        val stats = PairedListStats<Result>("ols" to OLS(), "cov" to Covariance())
        stats.update(1.0, 2.0)
        stats.update(2.0, 4.0)

        val r = stats.read()
        assertEquals(listOf("ols", "cov"), r.names)
        val first = assertIs<OLSResult>(r.results[0])
        assertEquals(2.0, first.slope, DELTA)
    }

    @Test
    fun `duplicate names throw at construction`() {
        assertFailsWith<IllegalArgumentException> {
            PairedListStats<OLSResult>("a" to OLS(), "a" to OLS())
        }
    }

    @Test
    fun `pairedListStats factory auto-names by simpleName`() {
        val stats = pairedListStats<Result>(OLS(), Covariance())
        val map = stats.read().toMap()
        assertEquals(setOf("OLS", "Covariance"), map.keys)
    }

    @Test
    fun `pairedListStats factory rejects duplicate auto-names`() {
        assertFailsWith<IllegalArgumentException> {
            pairedListStats<OLSResult>(OLS(), OLS())
        }
    }

    @Test
    fun `reset clears all child stats`() {
        val stats = PairedListStats<OLSResult>("ols" to OLS())
        stats.update(1.0, 2.0)
        stats.update(3.0, 6.0)
        stats.reset()
        val r = stats.read()
        val first = assertIs<OLSResult>(r.results[0])
        assertEquals(0.0, first.totalWeights, DELTA)
    }

    @Test
    fun `create returns independent list`() {
        val original = PairedListStats<OLSResult>("ols" to OLS())
        original.update(1.0, 2.0)
        val clone = original.create()
        clone.update(3.0, 6.0)

        val origFirst = assertIs<OLSResult>(original.read().results[0])
        val cloneFirst = assertIs<OLSResult>(clone.read().results[0])
        assertEquals(1.0, origFirst.totalWeights, DELTA)
        assertEquals(1.0, cloneFirst.totalWeights, DELTA)
    }

    @Test
    fun `merge combines each position`() {
        val target = PairedListStats<OLSResult>("ols" to OLS())
        target.update(1.0, 2.0)

        val source = PairedListStats<OLSResult>("ols" to OLS())
        source.update(2.0, 4.0)
        source.update(3.0, 6.0)

        target.merge(source.read())
        val mergedFirst = assertIs<OLSResult>(target.read().results[0])
        assertEquals(3.0, mergedFirst.totalWeights, DELTA)
    }

    @Test
    fun `create uses list mode when create mode is null`() {
        var childCreateMode: StreamMode? = null

        val tracking = object : PairedStat<OLSResult> {
            override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: OLSResult) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) =
                OLSResult(0.0, 0.0, 0.0, 0.0, VarianceResult(0.0, 0.0), VarianceResult(0.0, 0.0))
            override fun create(mode: StreamMode?): PairedStat<OLSResult> {
                childCreateMode = mode
                return this
            }
        }

        val stats = PairedListStats(listOf("t" to tracking), mode = SerialMode)
        stats.create(null)
        assertSame(SerialMode, childCreateMode)
    }

    @Test
    fun `input x and y are forwarded in order`() {
        val stats = PairedListStats<OLSResult>("ols" to OLS())
        stats.update(1.0, 2.0)
        stats.update(2.0, 4.0)
        stats.update(3.0, 6.0)
        val result = assertIs<OLSResult>(stats.read().results[0])
        assertTrue(result.slope > 0.0)
    }
}
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
class DiscreteStatGroupTest {

    @Test
    fun `update fans out to all child stats`() {
        val hllKey = StatKey<HyperLogLogResult>("hll") to HyperLogLogPlus(precision = 10)
        val lcKey = StatKey<LinearCountingResult>("lc") to LinearCounting(bits = 1024)

        val group = DiscreteStatGroup(hllKey, lcKey)
        for (i in 1L..100L) group.update(i)

        val result = group.read()
        assertEquals(2, result.results.size)
        assertTrue(result[hllKey.first].estimate > 50.0)
        assertTrue(result[lcKey.first].estimate > 50.0)
    }

    @Test
    fun `read returns GroupResult keyed by name`() {
        val key = StatKey<HyperLogLogResult>("hll") to HyperLogLogPlus(precision = 10)
        val group = DiscreteStatGroup(key)
        group.update(1L)
        val r = group.read()
        assertEquals(setOf("hll"), r.results.keys)
    }

    @Test
    fun `merge dispatches per stat`() {
        val hllKey = StatKey<HyperLogLogResult>("hll") to HyperLogLogPlus(precision = 10)
        val target = DiscreteStatGroup(hllKey)
        target.update(1L)

        val sourceKey = StatKey<HyperLogLogResult>("hll") to HyperLogLogPlus(precision = 10)
        val source = DiscreteStatGroup(sourceKey)
        for (i in 100L..200L) source.update(i)

        target.merge(source.read())
        // After merge, target should see ~102 distinct keys.
        assertTrue(target.read()[hllKey.first].estimate > 50.0)
    }

    @Test
    fun `reset clears all child stats`() {
        val key = StatKey<HyperLogLogResult>("hll") to HyperLogLogPlus(precision = 10)
        val group = DiscreteStatGroup(key)
        for (i in 1L..100L) group.update(i)
        group.reset()
        assertEquals(0.0, group.read()[key.first].estimate)
    }

    @Test
    fun `create returns an independent group`() {
        val key = StatKey<HyperLogLogResult>("hll") to HyperLogLogPlus(precision = 10)
        val original = DiscreteStatGroup(key)
        for (i in 1L..50L) original.update(i)

        val clone = original.create()
        clone.update(999L)

        assertTrue(original.read()[key.first].estimate > 30.0)
        assertTrue(clone.read()[key.first].estimate < 5.0)
    }

    @Test
    fun `create uses group mode when create mode is null`() {
        var childCreateMode: StreamMode? = null

        val tracking = object : DiscreteStat<HyperLogLogResult> {
            override fun update(value: Long, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: HyperLogLogResult) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) =
                HyperLogLogResult(0.0, 10, IntArray(0), 0L)
            override fun create(mode: StreamMode?): DiscreteStat<HyperLogLogResult> {
                childCreateMode = mode
                return this
            }
        }

        val group = DiscreteStatGroup(
            StatKey<HyperLogLogResult>("h") to tracking,
            mode = SerialMode
        )
        group.create(null)
        assertSame(SerialMode, childCreateMode)
    }

    @Test
    fun `StatSchema discrete delegate exposes typed keys`() {
        class Schema : StatSchema() {
            val users by discrete(HyperLogLogPlus(precision = 10))
            val sessions by discrete(LinearCounting(bits = 1024))
        }

        val schema = Schema()
        val group = DiscreteStatGroup(
            stats = schema.specs.map {
                @Suppress("UNCHECKED_CAST")
                StatSpec(it.key as StatKey<Result>, it.stat as DiscreteStat<Result>)
            }
        )
        for (i in 1L..50L) group.update(i)
        assertTrue(group.read()[schema.users].estimate > 30.0)
        assertTrue(group.read()[schema.sessions].estimate > 30.0)
    }
}

class DiscreteListStatsTest {

    @Test
    fun `update forwards to all child stats`() {
        val stats = DiscreteListStats<Result>(
            "hll" to HyperLogLogPlus(precision = 10),
            "lc" to LinearCounting(bits = 1024),
        )
        for (i in 1L..100L) stats.update(i)

        val r = stats.read()
        assertEquals(listOf("hll", "lc"), r.names)
        assertTrue(assertIs<HyperLogLogResult>(r.results[0]).estimate > 50.0)
        assertTrue(assertIs<LinearCountingResult>(r.results[1]).estimate > 50.0)
    }

    @Test
    fun `duplicate names throw at construction`() {
        assertFailsWith<IllegalArgumentException> {
            DiscreteListStats<HyperLogLogResult>(
                "a" to HyperLogLogPlus(precision = 10),
                "a" to HyperLogLogPlus(precision = 10),
            )
        }
    }

    @Test
    fun `discreteListStats factory auto-names by simpleName`() {
        val stats = discreteListStats<Result>(
            HyperLogLogPlus(precision = 10),
            LinearCounting(bits = 1024),
        )
        val map = stats.read().toMap()
        assertEquals(setOf("HyperLogLogPlus", "LinearCounting"), map.keys)
    }

    @Test
    fun `discreteListStats factory rejects duplicate auto-names`() {
        assertFailsWith<IllegalArgumentException> {
            discreteListStats<HyperLogLogResult>(
                HyperLogLogPlus(precision = 10),
                HyperLogLogPlus(precision = 10),
            )
        }
    }

    @Test
    fun `reset clears all child stats`() {
        val stats = DiscreteListStats<HyperLogLogResult>("h" to HyperLogLogPlus(precision = 10))
        for (i in 1L..50L) stats.update(i)
        stats.reset()
        val first = assertIs<HyperLogLogResult>(stats.read().results[0])
        assertEquals(0.0, first.estimate)
    }

    @Test
    fun `create returns independent list`() {
        val original = DiscreteListStats<HyperLogLogResult>("h" to HyperLogLogPlus(precision = 10))
        for (i in 1L..50L) original.update(i)

        val clone = original.create()
        clone.update(999L)

        val origFirst = assertIs<HyperLogLogResult>(original.read().results[0])
        val cloneFirst = assertIs<HyperLogLogResult>(clone.read().results[0])
        assertTrue(origFirst.estimate > 30.0)
        assertTrue(cloneFirst.estimate < 5.0)
    }

    @Test
    fun `merge combines each position`() {
        val target = DiscreteListStats<HyperLogLogResult>("h" to HyperLogLogPlus(precision = 10))
        for (i in 1L..50L) target.update(i)

        val source = DiscreteListStats<HyperLogLogResult>("h" to HyperLogLogPlus(precision = 10))
        for (i in 100L..200L) source.update(i)

        target.merge(source.read())
        val merged = assertIs<HyperLogLogResult>(target.read().results[0])
        assertTrue(merged.estimate > 100.0, "estimate=${merged.estimate}")
    }
}
