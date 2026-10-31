package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.skema.SchemaJson
import kotlinx.serialization.encodeToString
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MadStatTest {

    @Test
    fun `read before any update reports NaN estimates`() {
        val result = MadStat().read()

        assertTrue(result.median.isNaN())
        assertTrue(result.mad.isNaN())
    }

    @Test
    fun `compression must be positive`() {
        for (compression in listOf(0.0, -1.0)) {
            assertFailsWith<IllegalArgumentException> { MadStat(compression) }
        }
    }

    @Test
    fun `all deviations use the same median regardless of input order`() {
        for (values in listOf(
            listOf(0.0, 100.0, 200.0),
            listOf(200.0, 100.0, 0.0),
            listOf(100.0, 0.0, 200.0),
        )) {
            val stat = MadStat()
            values.forEach { stat.update(it) }

            assertEquals(100.0, stat.read().median, DELTA)
            assertEquals(100.0, stat.read().mad, DELTA)
        }
    }

    @Test
    fun `even samples average the two middle deviations`() {
        val stat = MadStat()
        listOf(0.0, 10.0, 20.0, 30.0).forEach { stat.update(it) }

        assertEquals(10.0, stat.read().mad, DELTA)
    }

    @Test
    fun `weighted medians agree with repeated observations`() {
        val weighted = MadStat().apply {
            update(0.0)
            update(10.0, weight = 2.0)
            update(100.0)
        }
        val repeated = MadStat().apply {
            listOf(0.0, 10.0, 10.0, 100.0).forEach { update(it) }
        }

        assertEquals(repeated.read().median, weighted.read().median, DELTA)
        assertEquals(5.0, weighted.read().mad, DELTA)
    }

    @Test
    fun `a majority atom determines the weighted center and deviation`() {
        val stat = MadStat().apply {
            update(0.0, weight = 100.0)
            update(100.0)
        }

        assertEquals(0.0, stat.read().median, DELTA)
        assertEquals(0.0, stat.read().mad, DELTA)
    }

    @Test
    fun `median tracks the central value of a symmetric stream`() {
        val stat = MadStat(compression = 200.0)
        repeat(1000) { stat.update((it % 100).toDouble()) }

        assertEquals(49.5, stat.read().median, 2.0)
    }

    @Test
    fun `mad approaches the theoretical value on a centred stream`() {
        val stat = MadStat(compression = 200.0)
        val n = 5_000
        repeat(n) { stat.update(-1.0 + 2.0 * it / (n - 1)) }

        assertEquals(0.5, stat.read().mad, 0.03)
    }

    @Test
    fun `merge retains unequal shard weights`() {
        val local = MadStat().apply { update(0.0) }
        val remote = MadStat().apply {
            update(10.0, weight = 10.0)
            update(20.0, weight = 10.0)
        }

        local.merge(remote.read())

        assertEquals(10.0, local.read().median, DELTA)
        assertEquals(10.0, local.read().mad, DELTA)
        assertEquals(21.0, local.read().digest!!.totalWeights, DELTA)
    }

    @Test
    fun `serialized snapshots retain merge evidence`() {
        val source = MadStat().apply { listOf(0.0, 100.0, 200.0).forEach { update(it) } }
        val decoded = SchemaJson.decodeFromString<MadResult>(SchemaJson.encodeToString(source.read()))
        val target = MadStat()

        target.merge(decoded)

        assertEquals(100.0, target.read().mad, DELTA)
    }

    @Test
    fun `summary-only results merge as a unit-weight symmetric distribution`() {
        val stat = MadStat()

        stat.merge(MadResult(10.0, 3.0))
        val result = stat.read()

        assertEquals(10.0, result.median, DELTA)
        assertEquals(3.0, result.mad, DELTA)
        assertEquals(1.0, result.digest!!.totalWeight, DELTA)
    }

    @Test
    fun `merging an empty summary leaves the digest unchanged`() {
        val stat = MadStat().apply { update(7.0) }
        val before = stat.read()

        stat.merge(FrugalMadStat().read())

        assertEquals(before, stat.read())
    }

    @Test
    fun `incompatible digest merges are rejected before changing state`() {
        val stat = MadStat(compression = 100.0).apply { update(7.0) }
        val before = stat.read()
        val incoming = MadStat(compression = 200.0).apply { update(42.0) }.read()

        assertFailsWith<IllegalArgumentException> { stat.validateMerge(incoming) }
        assertFailsWith<IllegalArgumentException> { stat.merge(incoming) }

        assertEquals(before, stat.read())
    }

    @Test
    fun `digest projection does not require a precomputed median`() {
        val digest = TDigestStat(probabilities = doubleArrayOf(0.9)).apply {
            listOf(0.0, 100.0, 200.0).forEach { update(it) }
        }.read()

        val result = digest.medianAbsoluteDeviation()

        assertEquals(100.0, result.mad, DELTA)
        assertEquals(listOf(0.9), digest.probabilities.toList())
    }

    @Test
    fun `centroid projection matches two-pass MAD on weighted discrete samples`() {
        val random = Random(42)
        repeat(32) {
            val means = DoubleArray(12) { random.nextInt(-10, 11).toDouble() }.sortedArray()
            val weights = DoubleArray(12) { random.nextInt(1, 5).toDouble() }
            val expanded = means.indices.flatMap { i -> List(weights[i].toInt()) { means[i] } }.sorted()
            val middle = expanded.size / 2
            val median = if (expanded.size % 2 == 0) {
                expanded[middle - 1] / 2.0 + expanded[middle] / 2.0
            } else {
                expanded[middle]
            }
            val deviations = expanded.map { abs(it - median) }.sorted()
            val expected = if (deviations.size % 2 == 0) {
                deviations[middle - 1] / 2.0 + deviations[middle] / 2.0
            } else {
                deviations[middle]
            }
            val digest = TDigestResult(
                doubleArrayOf(),
                doubleArrayOf(),
                means,
                weights,
                weights.sum(),
                100.0,
            )

            val result = digest.medianAbsoluteDeviation()

            assertEquals(median, result.median, DELTA)
            assertEquals(expected, result.mad, DELTA)
        }
    }

    @Test
    fun `fractional weights preserve median and MAD under common scaling`() {
        val results = listOf(1.0, 0.01, 100.0).map { scale ->
            MadStat().apply {
                update(0.0, weight = scale)
                update(10.0, weight = 2.0 * scale)
                update(100.0, weight = scale)
            }.read()
        }

        results.forEach {
            assertEquals(10.0, it.median, DELTA)
            assertEquals(5.0, it.mad, DELTA)
        }
    }

    @Test
    fun `infinite observations leave the median finite`() {
        val m = MadStat()
        repeat(20_000) { m.update(it.toDouble()) }
        repeat(200) { m.update(Double.POSITIVE_INFINITY) }
        assertTrue(m.read().median.isFinite(), "median=${m.read().median}")
    }
}
