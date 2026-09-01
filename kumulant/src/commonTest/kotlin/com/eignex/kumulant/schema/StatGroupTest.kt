package com.eignex.kumulant.schema

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.feat
import com.eignex.kumulant.operation.VectorizedStat
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.cardinality.LinearCountingResult
import com.eignex.kumulant.stat.cardinality.LinearCountingStat
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.CovarianceStat
import com.eignex.kumulant.stat.regression.glm.Penalty
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

private fun sumVector(d: Int) = VectorizedStat(d, SumStat())
private fun meanVector(d: Int) = VectorizedStat(d, MeanStat())

class StatGroupTest {

    @Test
    fun `update forwards values to all child stats and read returns grouped result`() {
        val sum = StatKey<SumResult>("sum") to SumStat()
        val count = StatKey<SumResult>("count") to SumStat().withValue(1.0).withWeight(1.0)

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
        val sum = StatKey<SumResult>("sum") to SumStat()
        val count = StatKey<SumResult>("count") to SumStat().withValue(1.0).withWeight(1.0)

        val target = StatGroup(sum, count)
        target.update(10.0)

        val source = StatGroup(
            StatKey<SumResult>("sum") to SumStat(),
            StatKey<SumResult>("count") to SumStat().withValue(1.0).withWeight(1.0),
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
        val sum = StatKey<SumResult>("sum") to SumStat()
        val count = StatKey<SumResult>("count") to SumStat().withValue(1.0).withWeight(1.0)

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
        val sum = StatKey<SumResult>("sum") to SumStat()
        val count = StatKey<SumResult>("count") to SumStat().withValue(1.0).withWeight(1.0)

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
        val nestedSum = StatKey<SumResult>("leafSum") to SumStat()
        val nestedCount = StatKey<SumResult>("leafCount") to SumStat().withValue(1.0).withWeight(1.0)
        val nested = StatKey<GroupResult>("nested") to StatGroup(nestedSum, nestedCount)
        val topSum = StatKey<SumResult>("topSum") to SumStat()

        val target = StatGroup(nested, topSum)

        target.update(2.0, timestampNanos = 10L, weight = 2.0)
        target.update(3.0, timestampNanos = 20L, weight = 1.0)

        val read = target.read(30L)
        assertEquals(7.0, read[topSum.first].sum, DELTA)
        assertEquals(7.0, read[nested.first][nestedSum.first].sum, DELTA)
        assertEquals(2.0, read[nested.first][nestedCount.first].sum, DELTA)

        val source = StatGroup(
            StatKey<GroupResult>("nested") to StatGroup(
                StatKey<SumResult>("leafSum") to SumStat(),
                StatKey<SumResult>("leafCount") to SumStat().withValue(1.0).withWeight(1.0),
            ),
            StatKey<SumResult>("topSum") to SumStat(),
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
        val topSum = StatKey<SumResult>("sum") to SumStat()
        val nestedSum = StatKey<SumResult>("sum") to SumStat()
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
        val httpCount = StatKey<SumResult>("count") to SumStat().withValue(1.0).withWeight(1.0)
        val httpTotalMs = StatKey<SumResult>("totalMs") to SumStat()
        val http = StatKey<GroupResult>("http") to StatGroup(httpCount, httpTotalMs)

        val dbCount = StatKey<SumResult>("count") to SumStat().withValue(1.0).withWeight(1.0)
        val dbTotalMs = StatKey<SumResult>("totalMs") to SumStat()
        val db = StatKey<GroupResult>("db") to StatGroup(dbCount, dbTotalMs)

        val requests = StatKey<SumResult>("requests") to SumStat().withValue(1.0).withWeight(1.0)

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
        var childCreateConcurrency: Concurrency? = null

        val tracking = object : SeriesStat<SumResult> {
            override val concurrency: Concurrency = Concurrency.None
            override fun update(value: Double, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: SumResult, workspace: com.eignex.koblas.Workspace?) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = SumResult(0.0)
            override fun create(concurrency: Concurrency?): SeriesStat<SumResult> {
                childCreateConcurrency = concurrency
                return this
            }
        }

        val group = StatGroup(
            StatKey<SumResult>("sum") to tracking,
            concurrency = Concurrency.None,
        )

        group.create(null)

        assertSame(Concurrency.None, childCreateConcurrency)
    }

    @Test
    fun `stat schema helper supports namespaced composition and lifecycle operations`() {
        class HttpMetrics : StatSchema() {
            val requests by series(Sum.withValue(1.0).withWeight(1.0))
            val latencyMsTotal by series(Sum)
        }

        class DbMetrics : StatSchema() {
            val requests by series(Sum.withValue(1.0).withWeight(1.0))
            val latencyMsTotal by series(Sum)
        }

        class ServiceMetrics : StatSchema() {
            val requests by series(Sum.withValue(1.0).withWeight(1.0))
            val billableMsTotal by series(Sum)
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

    @Test
    fun `StatGroup rejects duplicate stat names`() {
        assertFailsWith<IllegalArgumentException> {
            StatGroup(StatKey<SumResult>("hits") to SumStat(), StatKey<SumResult>("hits") to SumStat())
        }
    }
}

class PairedStatGroupTest {

    @Test
    fun `update forwards x and y pairs to all child stats`() {
        val ols = StatKey<UnivariateRegressionResult>("ols") to UnivariateRegressionStat()
        val cov = StatKey<CovarianceResult>("cov") to CovarianceStat()

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
        val olsKey = StatKey<UnivariateRegressionResult>("ols") to UnivariateRegressionStat()
        val covKey = StatKey<CovarianceResult>("cov") to CovarianceStat()

        val target = PairedStatGroup(olsKey, covKey)
        target.update(1.0, 2.0)

        val source = PairedStatGroup(
            StatKey<UnivariateRegressionResult>("ols") to UnivariateRegressionStat(),
            StatKey<CovarianceResult>("cov") to CovarianceStat(),
        )
        source.update(2.0, 4.0)
        source.update(3.0, 6.0)

        val onlyOls = GroupResult(results = mapOf("ols" to source.read()[StatKey<UnivariateRegressionResult>("ols")]))

        target.merge(onlyOls)
        val merged = target.read()
        assertEquals(3.0, merged[olsKey.first].totalWeights, DELTA)

        assertEquals(1.0, merged[covKey.first].totalWeights, DELTA)
    }

    @Test
    fun `reset clears all child stats`() {
        val olsKey = StatKey<UnivariateRegressionResult>("ols") to UnivariateRegressionStat()
        val group = PairedStatGroup(olsKey)
        group.update(1.0, 2.0)
        group.update(2.0, 4.0)
        group.reset()
        assertEquals(0.0, group.read()[olsKey.first].totalWeights, DELTA)
    }

    @Test
    fun `create returns an independent group`() {
        val olsKey = StatKey<UnivariateRegressionResult>("ols") to UnivariateRegressionStat()
        val original = PairedStatGroup(olsKey)
        original.update(1.0, 2.0)

        val clone = original.create()
        clone.update(3.0, 6.0)

        assertEquals(1.0, original.read()[olsKey.first].totalWeights, DELTA)
        assertEquals(1.0, clone.read()[olsKey.first].totalWeights, DELTA)
    }

    @Test
    fun `create uses group mode when create mode is null`() {
        var childCreateConcurrency: Concurrency? = null

        val tracking = object : PairedStat<UnivariateRegressionResult> {
            override val concurrency: Concurrency = Concurrency.None
            override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: UnivariateRegressionResult, workspace: com.eignex.koblas.Workspace?) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = UnivariateRegressionResult(
                Penalty.None,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                VarianceResult(0.0, 0.0),
                VarianceResult(0.0, 0.0),
            )
            override fun create(concurrency: Concurrency?): PairedStat<UnivariateRegressionResult> {
                childCreateConcurrency = concurrency
                return this
            }
        }

        val group = PairedStatGroup(
            StatKey<UnivariateRegressionResult>("ols") to tracking,
            concurrency = Concurrency.None,
        )
        group.create(null)
        assertSame(Concurrency.None, childCreateConcurrency)
    }

    @Test
    fun `paired schema constructor materializes config entries`() {
        val schema = object : StatSchema() {
            val olsKey by paired(UnivariateRegression())
            val covKey by paired(Covariance)
        }
        val group = PairedStatGroup(schema)
        group.update(1.0, 2.0)
        group.update(2.0, 4.0)
        val r = group.read()
        assertEquals(2.0, r[schema.olsKey].slope, DELTA)
    }
}

class PairedListStatsTest {

    @Test
    fun `update forwards to all child stats`() {
        val stats = PairedListStats<Result>("ols" to UnivariateRegressionStat(), "cov" to CovarianceStat())
        stats.update(1.0, 2.0)
        stats.update(2.0, 4.0)

        val r = stats.read()
        assertEquals(listOf("ols", "cov"), r.names)
        val first = assertIs<UnivariateRegressionResult>(r.results[0])
        assertEquals(2.0, first.slope, DELTA)
    }

    @Test
    fun `duplicate names throw at construction`() {
        assertFailsWith<IllegalArgumentException> {
            PairedListStats<UnivariateRegressionResult>(
                "a" to UnivariateRegressionStat(),
                "a" to UnivariateRegressionStat(),
            )
        }
    }

    @Test
    fun `pairedListStats factory auto-names by simpleName`() {
        val stats = pairedListStats<Result>(UnivariateRegressionStat(), CovarianceStat())
        val map = stats.read().toMap()
        assertEquals(setOf("UnivariateRegressionStat", "CovarianceStat"), map.keys)
    }

    @Test
    fun `pairedListStats factory rejects duplicate auto-names`() {
        assertFailsWith<IllegalArgumentException> {
            pairedListStats<UnivariateRegressionResult>(UnivariateRegressionStat(), UnivariateRegressionStat())
        }
    }

    @Test
    fun `reset clears all child stats`() {
        val stats = PairedListStats<UnivariateRegressionResult>("ols" to UnivariateRegressionStat())
        stats.update(1.0, 2.0)
        stats.update(3.0, 6.0)
        stats.reset()
        val r = stats.read()
        val first = assertIs<UnivariateRegressionResult>(r.results[0])
        assertEquals(0.0, first.totalWeights, DELTA)
    }

    @Test
    fun `create returns independent list`() {
        val original = PairedListStats<UnivariateRegressionResult>("ols" to UnivariateRegressionStat())
        original.update(1.0, 2.0)
        val clone = original.create()
        clone.update(3.0, 6.0)

        val origFirst = assertIs<UnivariateRegressionResult>(original.read().results[0])
        val cloneFirst = assertIs<UnivariateRegressionResult>(clone.read().results[0])
        assertEquals(1.0, origFirst.totalWeights, DELTA)
        assertEquals(1.0, cloneFirst.totalWeights, DELTA)
    }

    @Test
    fun `merge combines each position`() {
        val target = PairedListStats<UnivariateRegressionResult>("ols" to UnivariateRegressionStat())
        target.update(1.0, 2.0)

        val source = PairedListStats<UnivariateRegressionResult>("ols" to UnivariateRegressionStat())
        source.update(2.0, 4.0)
        source.update(3.0, 6.0)

        target.merge(source.read())
        val mergedFirst = assertIs<UnivariateRegressionResult>(target.read().results[0])
        assertEquals(3.0, mergedFirst.totalWeights, DELTA)
    }

    @Test
    fun `create uses list mode when create mode is null`() {
        var childCreateConcurrency: Concurrency? = null

        val tracking = object : PairedStat<UnivariateRegressionResult> {
            override val concurrency: Concurrency = Concurrency.None
            override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: UnivariateRegressionResult, workspace: com.eignex.koblas.Workspace?) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = UnivariateRegressionResult(
                Penalty.None,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                VarianceResult(0.0, 0.0),
                VarianceResult(0.0, 0.0),
            )
            override fun create(concurrency: Concurrency?): PairedStat<UnivariateRegressionResult> {
                childCreateConcurrency = concurrency
                return this
            }
        }

        val stats = PairedListStats(listOf("t" to tracking), concurrency = Concurrency.None)
        stats.create(null)
        assertSame(Concurrency.None, childCreateConcurrency)
    }

    @Test
    fun `input x and y are forwarded in order`() {
        val stats = PairedListStats<UnivariateRegressionResult>("ols" to UnivariateRegressionStat())
        stats.update(1.0, 2.0)
        stats.update(2.0, 4.0)
        stats.update(3.0, 6.0)
        val result = assertIs<UnivariateRegressionResult>(stats.read().results[0])
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
            StatKey<ResultList<WeightedMeanResult>>("means") to meanVector(2),
        )
        source.update(doubleArrayOf(2.0, 20.0))
        source.update(doubleArrayOf(3.0, 30.0))

        val onlySums = GroupResult(
            results = mapOf("sums" to source.read()[StatKey<ResultList<SumResult>>("sums")]),
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
        var childCreateConcurrency: Concurrency? = null

        val tracking = object : VectorStat<ResultList<SumResult>> {
            override val concurrency: Concurrency = Concurrency.None
            override fun update(vector: F64VectorLike, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: ResultList<SumResult>, workspace: com.eignex.koblas.Workspace?) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = ResultList<SumResult>(emptyList())
            override fun create(concurrency: Concurrency?): VectorStat<ResultList<SumResult>> {
                childCreateConcurrency = concurrency
                return this
            }
        }

        val group = VectorStatGroup(
            StatKey<ResultList<SumResult>>("v") to tracking,
            concurrency = Concurrency.None,
        )
        group.create(null)
        assertSame(Concurrency.None, childCreateConcurrency)
    }

    @Test
    fun `vector schema constructor materializes config entries`() {
        val schema = object : StatSchema() {
            val vsumKey by vector(Sum.vectorized(dimensions = 2))
        }
        val group = VectorStatGroup(schema)
        group.update(doubleArrayOf(1.0, 10.0))
        group.update(doubleArrayOf(3.0, 30.0))
        val r = group.read()
        assertTrue(r.results.containsKey("vsumKey"))
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
                "a" to sumVector(2),
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
        var childCreateConcurrency: Concurrency? = null

        val tracking = object : VectorStat<ResultList<SumResult>> {
            override val concurrency: Concurrency = Concurrency.None
            override fun update(vector: F64VectorLike, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: ResultList<SumResult>, workspace: com.eignex.koblas.Workspace?) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = ResultList<SumResult>(emptyList())
            override fun create(concurrency: Concurrency?): VectorStat<ResultList<SumResult>> {
                childCreateConcurrency = concurrency
                return this
            }
        }

        val stats = VectorListStats(listOf("t" to tracking), concurrency = Concurrency.None)
        stats.create(null)
        assertSame(Concurrency.None, childCreateConcurrency)
    }
}
class DiscreteStatGroupTest {

    @Test
    fun `update fans out to all child stats`() {
        val hllKey = StatKey<HyperLogLogResult>("hll") to HyperLogLogStat(precision = 10)
        val lcKey = StatKey<LinearCountingResult>("lc") to LinearCountingStat(bits = 1024)

        val group = DiscreteStatGroup(hllKey, lcKey)
        for (i in 1L..100L) group.update(i)

        val result = group.read()
        assertEquals(2, result.results.size)
        assertTrue(result[hllKey.first].estimate > 50.0)
        assertTrue(result[lcKey.first].estimate > 50.0)
    }

    @Test
    fun `read returns GroupResult keyed by name`() {
        val key = StatKey<HyperLogLogResult>("hll") to HyperLogLogStat(precision = 10)
        val group = DiscreteStatGroup(key)
        group.update(1L)
        val r = group.read()
        assertEquals(setOf("hll"), r.results.keys)
    }

    @Test
    fun `merge dispatches per stat`() {
        val hllKey = StatKey<HyperLogLogResult>("hll") to HyperLogLogStat(precision = 10)
        val target = DiscreteStatGroup(hllKey)
        target.update(1L)

        val sourceKey = StatKey<HyperLogLogResult>("hll") to HyperLogLogStat(precision = 10)
        val source = DiscreteStatGroup(sourceKey)
        for (i in 100L..200L) source.update(i)

        target.merge(source.read())
        // After merge, target should see ~102 distinct keys.
        assertTrue(target.read()[hllKey.first].estimate > 50.0)
    }

    @Test
    fun `reset clears all child stats`() {
        val key = StatKey<HyperLogLogResult>("hll") to HyperLogLogStat(precision = 10)
        val group = DiscreteStatGroup(key)
        for (i in 1L..100L) group.update(i)
        group.reset()
        assertEquals(0.0, group.read()[key.first].estimate)
    }

    @Test
    fun `create returns an independent group`() {
        val key = StatKey<HyperLogLogResult>("hll") to HyperLogLogStat(precision = 10)
        val original = DiscreteStatGroup(key)
        for (i in 1L..50L) original.update(i)

        val clone = original.create()
        clone.update(999L)

        assertTrue(original.read()[key.first].estimate > 30.0)
        assertTrue(clone.read()[key.first].estimate < 5.0)
    }

    @Test
    fun `create uses group mode when create mode is null`() {
        var childCreateConcurrency: Concurrency? = null

        val tracking = object : DiscreteStat<HyperLogLogResult> {
            override val concurrency: Concurrency = Concurrency.None
            override fun update(value: Long, timestampNanos: Long, weight: Double) = Unit
            override fun merge(values: HyperLogLogResult, workspace: com.eignex.koblas.Workspace?) = Unit
            override fun reset() = Unit
            override fun read(timestampNanos: Long) = HyperLogLogResult(0.0, 10, IntArray(0), 0L)
            override fun create(concurrency: Concurrency?): DiscreteStat<HyperLogLogResult> {
                childCreateConcurrency = concurrency
                return this
            }
        }

        val group = DiscreteStatGroup(
            StatKey<HyperLogLogResult>("h") to tracking,
            concurrency = Concurrency.None,
        )
        group.create(null)
        assertSame(Concurrency.None, childCreateConcurrency)
    }

    @Test
    fun `StatSchema discrete delegate exposes typed keys`() {
        class Schema : StatSchema() {
            val users by discrete(HyperLogLog(precision = 10))
            val sessions by discrete(LinearCounting(bits = 1024))
        }

        val schema = Schema()
        val group = DiscreteStatGroup(schema)
        for (i in 1L..50L) group.update(i)
        assertTrue(group.read()[schema.users].estimate > 30.0)
        assertTrue(group.read()[schema.sessions].estimate > 30.0)
    }
}

class DiscreteListStatsTest {

    @Test
    fun `update forwards to all child stats`() {
        val stats = DiscreteListStats<Result>(
            "hll" to HyperLogLogStat(precision = 10),
            "lc" to LinearCountingStat(bits = 1024),
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
                "a" to HyperLogLogStat(precision = 10),
                "a" to HyperLogLogStat(precision = 10),
            )
        }
    }

    @Test
    fun `discreteListStats factory auto-names by simpleName`() {
        val stats = discreteListStats<Result>(
            HyperLogLogStat(precision = 10),
            LinearCountingStat(bits = 1024),
        )
        val map = stats.read().toMap()
        assertEquals(setOf("HyperLogLogStat", "LinearCountingStat"), map.keys)
    }

    @Test
    fun `discreteListStats factory rejects duplicate auto-names`() {
        assertFailsWith<IllegalArgumentException> {
            discreteListStats<HyperLogLogResult>(
                HyperLogLogStat(precision = 10),
                HyperLogLogStat(precision = 10),
            )
        }
    }

    @Test
    fun `reset clears all child stats`() {
        val stats = DiscreteListStats<HyperLogLogResult>("h" to HyperLogLogStat(precision = 10))
        for (i in 1L..50L) stats.update(i)
        stats.reset()
        val first = assertIs<HyperLogLogResult>(stats.read().results[0])
        assertEquals(0.0, first.estimate)
    }

    @Test
    fun `create returns independent list`() {
        val original = DiscreteListStats<HyperLogLogResult>("h" to HyperLogLogStat(precision = 10))
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
        val target = DiscreteListStats<HyperLogLogResult>("h" to HyperLogLogStat(precision = 10))
        for (i in 1L..50L) target.update(i)

        val source = DiscreteListStats<HyperLogLogResult>("h" to HyperLogLogStat(precision = 10))
        for (i in 100L..200L) source.update(i)

        target.merge(source.read())
        val merged = assertIs<HyperLogLogResult>(target.read().results[0])
        assertTrue(merged.estimate > 100.0, "estimate=${merged.estimate}")
    }
}

class GroupMergeAtomicityTest {

    private object MergeHostile : StatSchema() {
        val hits by series(Sum)
        val p99 by series(QuantileFilter())
        val total by series(Count)
    }

    @Test
    fun `a refused merge leaves every entry untouched`() {
        val local = StatGroup(MergeHostile)
        val remote = StatGroup(MergeHostile)
        repeat(3) { local.update(1.0) }
        repeat(5) { remote.update(1.0) }

        val hitsBefore = local.read()[MergeHostile.hits].sum
        val totalBefore = local.read()[MergeHostile.total].sum

        assertFailsWith<UnsupportedOperationException> { local.merge(remote.read()) }

        assertEquals(hitsBefore, local.read()[MergeHostile.hits].sum, DELTA)
        assertEquals(totalBefore, local.read()[MergeHostile.total].sum, DELTA)
    }

    @Test
    fun `a group with no refusing entry still merges`() {
        val mergeable = object : StatSchema() {
            val hits by series(Sum)
            val total by series(Count)
        }
        val local = StatGroup(mergeable)
        val remote = StatGroup(mergeable)
        repeat(3) { local.update(1.0) }
        repeat(5) { remote.update(1.0) }
        local.merge(remote.read())
        assertEquals(8.0, local.read()[mergeable.hits].sum, DELTA)
        assertEquals(8.0, local.read()[mergeable.total].sum, DELTA)
    }
}

class BandAndTransformSpecGuardTest {

    @Test
    fun `band rejects an inner stat with no center and scale at materialize time`() {
        val schema = object : StatSchema() {
            val bad by series(Sum.band(2.0))
        }
        assertFailsWith<IllegalArgumentException> { StatGroup(schema) }
    }

    @Test
    fun `band still accepts an inner stat that has a center and scale`() {
        val schema = object : StatSchema() {
            val ok by series(Variance.band(2.0))
        }
        val group = StatGroup(schema)
        group.update(1.0)
        group.update(3.0)
        assertTrue(group.read()[schema.ok].upper >= group.read()[schema.ok].lower)
    }

    @Test
    fun `transformX reports the width callers must supply`() {
        val expanded = StochasticRegression(featureSize = 3)
            .transformX(vectorOf(V(0), V(1), V(0) * V(1)), featureSize = 2)
        val stat = expanded.materialize()
        assertEquals(2, stat.featureSize)
    }
}

class SchemaModalityReachabilityTest {

    private object MixedInner : StatSchema() {
        val hits by series(Sum)
        val users by discrete(HyperLogLog())
    }

    @Test
    fun `a nested schema holding a non-series entry is rejected at declaration`() {
        assertFailsWith<IllegalArgumentException> {
            val nested = object : StatSchema() {
                val inner by group(MixedInner)
            }
            nested.statSchemaDef()
        }
    }

    @Test
    fun `a schema-declared regression entry has a group to materialize into`() {
        val schema = object : StatSchema() {
            val model by regression(StochasticRegression(featureSize = 2))
        }
        val group = RegressionStatGroup(schema)
        group.update(feat(1.0, 2.0), 3.0)
        assertEquals(2, group.featureSize)
        assertTrue(group.read()[schema.model].totalWeights > 0.0)
    }
}
