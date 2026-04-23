package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.stat.Covariance
import com.eignex.kumulant.stat.OLS
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

private const val DELTA = 1e-9

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
        // cov was not in the incoming merge payload so it stays at 1 observation
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
