package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.regression.OLSResult
import com.eignex.kumulant.stat.summary.SumResult
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Schema-aware constructors for the four `*ListStats` classes. They walk the
 * matching modality helper (`seriesSpecs`/`pairedSpecs`/...) and turn it into
 * positional `(name, stat)` entries.
 */
private const val DELTA = 1e-12

class ListStatsSchemaTest {

    @Test fun seriesListStats_from_schema_preserves_order_and_results() {
        val schema = object : StatSchema() {
            val a by series(Sum)
            val b by series(Mean)
        }
        val list = ListStats<Result>(schema)
        list.update(2.0)
        list.update(4.0)
        val r = list.read()
        assertEquals(listOf("a", "b"), r.names)
        assertEquals(6.0, (r.results[0] as SumResult).sum, DELTA)
    }

    @Test fun pairedListStats_from_schema_preserves_order() {
        val schema = object : StatSchema() {
            val a by paired(OLS)
            val b by paired(Covariance)
        }
        val list = PairedListStats<Result>(schema)
        list.update(1.0, 2.0)
        list.update(2.0, 4.0)
        val r = list.read()
        assertEquals(listOf("a", "b"), r.names)
        assertEquals(2.0, (r.results[0] as OLSResult).slope, DELTA)
    }

    @Test fun discreteListStats_from_schema_preserves_order() {
        val schema = object : StatSchema() {
            val a by discrete(HyperLogLog(precision = 10))
            val b by discrete(LinearCounting(bits = 1024))
        }
        val list = DiscreteListStats<Result>(schema)
        for (i in 1L..50L) list.update(i)
        val r = list.read()
        assertEquals(listOf("a", "b"), r.names)
        kotlin.test.assertTrue((r.results[0] as HyperLogLogResult).estimate > 30.0)
    }

    @Test fun vectorListStats_from_schema_preserves_order() {
        val schema = object : StatSchema() {
            val a by vector(VarianceVector(dimensions = 2))
            val b by vector(VarianceVector(dimensions = 2))
        }
        val list = VectorListStats<Result>(schema)
        list.update(doubleArrayOf(1.0, 10.0))
        list.update(doubleArrayOf(3.0, 30.0))
        val r = list.read()
        assertEquals(listOf("a", "b"), r.names)
    }
}
