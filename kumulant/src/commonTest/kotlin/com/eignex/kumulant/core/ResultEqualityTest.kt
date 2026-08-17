package com.eignex.kumulant.core

import com.eignex.kumulant.bandit.contextual.Exp4State
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.cardinality.LinearCountingResult
import com.eignex.kumulant.stat.quantile.ReservoirResult
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.quantile.SparseHistogramResult
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.sketch.BloomFilterResult
import com.eignex.kumulant.stat.sketch.CountMinSketchResult
import com.eignex.kumulant.stat.sketch.HeavyHittersResult
import com.eignex.kumulant.stat.sketch.MinHashResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

// A Kotlin `data class` derives `equals` from its components and array components compare by
// reference, so the array-bearing results have to hand-roll it to keep the documented promise that
// results are structurally comparable.
class ResultEqualityTest {

    private fun sketch(quantiles: DoubleArray) = SketchResult(
        probabilities = doubleArrayOf(0.5, 0.9),
        quantiles = quantiles,
        gamma = 1.02,
        totalWeights = 10.0,
        zeroCount = 0.0,
        positiveBins = mapOf(1 to 4.0),
        negativeBins = emptyMap(),
    )

    @Test
    fun `identical snapshots compare equal and hash alike`() {
        val pairs: List<Pair<Result, Result>> = listOf(
            Exp4State(doubleArrayOf(1.0, 2.0)) to Exp4State(doubleArrayOf(1.0, 2.0)),
            HyperLogLogResult(1.5, 4, intArrayOf(1, 2, 3), 9L) to
                HyperLogLogResult(1.5, 4, intArrayOf(1, 2, 3), 9L),
            LinearCountingResult(2.0, 64, 60L, longArrayOf(7L), 4L) to
                LinearCountingResult(2.0, 64, 60L, longArrayOf(7L), 4L),
            sketch(doubleArrayOf(1.0, 9.0)) to sketch(doubleArrayOf(1.0, 9.0)),
            SparseHistogramResult(doubleArrayOf(0.0), doubleArrayOf(1.0), doubleArrayOf(3.0)) to
                SparseHistogramResult(doubleArrayOf(0.0), doubleArrayOf(1.0), doubleArrayOf(3.0)),
            ReservoirResult(doubleArrayOf(1.0), doubleArrayOf(0.5), 8, 3L, 3.0) to
                ReservoirResult(doubleArrayOf(1.0), doubleArrayOf(0.5), 8, 3L, 3.0),
            TDigestResult(doubleArrayOf(0.5), doubleArrayOf(2.0), doubleArrayOf(2.0), doubleArrayOf(1.0), 1.0, 100.0) to
                TDigestResult(
                    doubleArrayOf(0.5),
                    doubleArrayOf(2.0),
                    doubleArrayOf(2.0),
                    doubleArrayOf(1.0),
                    1.0,
                    100.0,
                ),
            BloomFilterResult(64, 3, longArrayOf(5L), 2L) to BloomFilterResult(64, 3, longArrayOf(5L), 2L),
            CountMinSketchResult(2, 4, 11L, longArrayOf(1L, 2L), 3L) to
                CountMinSketchResult(2, 4, 11L, longArrayOf(1L, 2L), 3L),
            MinHashResult(2, 7L, longArrayOf(9L, 8L), 5L) to MinHashResult(2, 7L, longArrayOf(9L, 8L), 5L),
            HeavyHittersResult(4, longArrayOf(1L), longArrayOf(2L), longArrayOf(0L), 2L) to
                HeavyHittersResult(4, longArrayOf(1L), longArrayOf(2L), longArrayOf(0L), 2L),
        )
        for ((a, b) in pairs) {
            assertTrue(a !== b, "${a::class.simpleName}: test built the same instance twice")
            assertEquals(a, b, "${a::class.simpleName} should compare structurally equal")
            assertEquals(a.hashCode(), b.hashCode(), "${a::class.simpleName} hashCode should agree")
        }
    }

    @Test
    fun `a difference inside an array makes snapshots unequal`() {
        assertNotEquals(sketch(doubleArrayOf(1.0, 9.0)), sketch(doubleArrayOf(1.0, 9.5)))
        assertNotEquals(
            HyperLogLogResult(1.5, 4, intArrayOf(1, 2, 3), 9L),
            HyperLogLogResult(1.5, 4, intArrayOf(1, 2, 4), 9L),
        )
        assertNotEquals(
            MinHashResult(2, 7L, longArrayOf(9L, 8L), 5L),
            MinHashResult(2, 7L, longArrayOf(9L, 7L), 5L),
        )
    }

    @Test
    fun `toString reports array contents when small and a summary when large`() {
        val small = HyperLogLogResult(1.0, 2, intArrayOf(1, 2), 2L).toString()
        assertTrue(small.contains("registers=[1, 2]"), "small arrays should be rendered: $small")

        // A real HLL register bank is 2^precision entries; dumping it would flood a log line.
        val large = HyperLogLogResult(1.0, 14, IntArray(16_384), 99L).toString()
        assertTrue(large.contains("registers=IntArray(size=16384)"), "large arrays should be summarised: $large")
        assertFalse(large.length > 200, "summarised toString should stay short, was ${large.length} chars")
    }
}
