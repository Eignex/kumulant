package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.stat.Count
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class StatGroupTest {

    @Test
    fun `update forwards values to all child stats and read returns grouped result`() {
        val sum = StatKey<SumResult>("sum") to Sum()
        val count = StatKey<CountResult>("count") to Count()

        val group = StatGroup(sum, count)

        group.update(2.0, timestampNanos = 10L, weight = 3.0)
        group.update(4.0, timestampNanos = 20L, weight = 0.5)

        val result = group.read(30L)
        assertEquals(2, result.results.size)
        assertEquals(8.0, result[sum.first].sum, DELTA)
        assertEquals(2L, result[count.first].count)
        assertTrue(result.results.containsKey("sum"))
        assertTrue(result.results.containsKey("count"))
    }

    @Test
    fun `merge merges only keys present in incoming grouped result`() {
        val sum = StatKey<SumResult>("sum") to Sum()
        val count = StatKey<CountResult>("count") to Count()

        val target = StatGroup(sum, count)
        target.update(10.0)

        val source = StatGroup(
            StatKey<SumResult>("sum") to Sum(),
            StatKey<CountResult>("count") to Count()
        )
        source.update(2.0)
        source.update(3.0)

        val incoming = GroupResult(results = mapOf("sum" to source.read()[StatKey("sum")]))

        target.merge(incoming)
        val merged = target.read()

        assertEquals(15.0, merged[sum.first].sum, DELTA)
        assertEquals(1L, merged[count.first].count)
    }

    @Test
    fun `reset resets all child stats`() {
        val sum = StatKey<SumResult>("sum") to Sum()
        val count = StatKey<CountResult>("count") to Count()

        val group = StatGroup(sum, count)
        group.update(5.0)
        group.update(1.0)

        group.reset()

        val result = group.read()
        assertEquals(0.0, result[sum.first].sum, DELTA)
        assertEquals(0L, result[count.first].count)
    }

    @Test
    fun `create returns independent group`() {
        val sum = StatKey<SumResult>("sum") to Sum()
        val count = StatKey<CountResult>("count") to Count()

        val original = StatGroup(sum, count)
        original.update(2.0)

        val created = original.create()
        created.update(3.0)

        val originalResult = original.read()
        val createdResult = created.read()
        assertEquals(2.0, originalResult[sum.first].sum, DELTA)
        assertEquals(1L, originalResult[count.first].count)
        assertEquals(3.0, createdResult[sum.first].sum, DELTA)
        assertEquals(1L, createdResult[count.first].count)
    }

    @Test
    fun `supports hierarchical stat groups`() {
        val nestedSum = StatKey<SumResult>("leafSum") to Sum()
        val nestedCount = StatKey<CountResult>("leafCount") to Count()
        val nested = StatKey<GroupResult>("nested") to StatGroup(nestedSum, nestedCount)
        val topSum = StatKey<SumResult>("topSum") to Sum()

        val target = StatGroup(nested, topSum)

        target.update(2.0, timestampNanos = 10L, weight = 2.0)
        target.update(3.0, timestampNanos = 20L, weight = 1.0)

        val read = target.read(30L)
        assertEquals(7.0, read[topSum.first].sum, DELTA)
        assertEquals(7.0, read[nested.first][nestedSum.first].sum, DELTA)
        assertEquals(2L, read[nested.first][nestedCount.first].count)

        val source = StatGroup(
            StatKey<GroupResult>("nested") to StatGroup(
                StatKey<SumResult>("leafSum") to Sum(),
                StatKey<CountResult>("leafCount") to Count()
            ),
            StatKey<SumResult>("topSum") to Sum()
        )
        source.update(5.0)

        target.merge(source.read())
        val merged = target.read()
        assertEquals(12.0, merged[topSum.first].sum, DELTA)
        assertEquals(12.0, merged[nested.first][nestedSum.first].sum, DELTA)
        assertEquals(3L, merged[nested.first][nestedCount.first].count)
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
        val httpCount = StatKey<CountResult>("count") to Count()
        val httpTotalMs = StatKey<SumResult>("totalMs") to Sum()
        val http = StatKey<GroupResult>("http") to StatGroup(httpCount, httpTotalMs)

        val dbCount = StatKey<CountResult>("count") to Count()
        val dbTotalMs = StatKey<SumResult>("totalMs") to Sum()
        val db = StatKey<GroupResult>("db") to StatGroup(dbCount, dbTotalMs)

        val requests = StatKey<CountResult>("requests") to Count()

        val service = StatGroup(http, db, requests)

        service.update(120.0)
        service.update(80.0)

        val read = service.read()
        assertEquals(2L, read[requests.first].count)
        assertEquals(2L, read[http.first][httpCount.first].count)
        assertEquals(200.0, read[http.first][httpTotalMs.first].sum, DELTA)
        assertEquals(2L, read[db.first][dbCount.first].count)
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
            val requests by stat(Count())
            val latencyMsTotal by stat(Sum())
        }

        class DbMetrics : StatSchema() {
            val requests by stat(Count())
            val latencyMsTotal by stat(Sum())
        }

        class ServiceMetrics : StatSchema() {
            val requests by stat(Count())
            val billableMsTotal by stat(Sum())
            val http by group(HttpMetrics())
            val db by group(DbMetrics())
        }

        val schema = ServiceMetrics()
        val service = StatGroup(schema)

        service.update(120.0)
        service.update(80.0)

        val firstRead = service.read()
        assertEquals(2L, firstRead[schema.requests].count)
        assertEquals(200.0, firstRead[schema.billableMsTotal].sum, DELTA)
        assertEquals(2L, firstRead[schema.http, { requests }].count)
        assertEquals(200.0, firstRead[schema.http, { latencyMsTotal }].sum, DELTA)
        assertEquals(2L, firstRead[schema.db, { requests }].count)
        assertEquals(200.0, firstRead[schema.db, { latencyMsTotal }].sum, DELTA)
        assertEquals(setOf("requests", "billableMsTotal", "http", "db"), firstRead.results.keys)
        assertTrue(firstRead.results["http"] is GroupResult)
        assertTrue(firstRead.results["db"] is GroupResult)

        val incoming = StatGroup(ServiceMetrics())
        incoming.update(50.0)
        incoming.update(30.0)
        service.merge(incoming.read())

        val merged = service.read()
        assertEquals(4L, merged[schema.requests].count)
        assertEquals(280.0, merged[schema.billableMsTotal].sum, DELTA)
        assertEquals(4L, merged[schema.http, { requests }].count)
        assertEquals(280.0, merged[schema.http, { latencyMsTotal }].sum, DELTA)
        assertEquals(4L, merged[schema.db, { requests }].count)
        assertEquals(280.0, merged[schema.db, { latencyMsTotal }].sum, DELTA)

        val cloned = service.create()
        val clonedBeforeUpdate = cloned.read()
        assertEquals(0L, clonedBeforeUpdate[schema.requests].count)
        assertEquals(0.0, clonedBeforeUpdate[schema.billableMsTotal].sum, DELTA)
        assertEquals(0L, clonedBeforeUpdate[schema.http, { requests }].count)
        assertEquals(0.0, clonedBeforeUpdate[schema.http, { latencyMsTotal }].sum, DELTA)

        cloned.update(10.0)

        assertEquals(4L, service.read()[schema.requests].count)
        assertEquals(1L, cloned.read()[schema.requests].count)
        assertEquals(10.0, cloned.read()[schema.billableMsTotal].sum, DELTA)
        assertEquals(1L, cloned.read()[schema.http, { requests }].count)
        assertEquals(10.0, cloned.read()[schema.http, { latencyMsTotal }].sum, DELTA)
        assertEquals(1L, cloned.read()[schema.db, { requests }].count)
        assertEquals(10.0, cloned.read()[schema.db, { latencyMsTotal }].sum, DELTA)

        service.reset()
        val reset = service.read()
        assertEquals(0L, reset[schema.requests].count)
        assertEquals(0.0, reset[schema.billableMsTotal].sum, DELTA)
        assertEquals(0L, reset[schema.http, { requests }].count)
        assertEquals(0.0, reset[schema.http, { latencyMsTotal }].sum, DELTA)
        assertEquals(0L, reset[schema.db, { requests }].count)
        assertEquals(0.0, reset[schema.db, { latencyMsTotal }].sum, DELTA)

        val clonedAfterReset = cloned.read()
        assertEquals(1L, clonedAfterReset[schema.requests].count)
        assertEquals(10.0, clonedAfterReset[schema.billableMsTotal].sum, DELTA)
    }
}
