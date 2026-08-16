package com.eignex.kumulant.stat.regression.tree

import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12

/**
 * The growth helpers both VFDT trees share, which each tree used to keep its own copy of.
 */
class TreeInternalsTest {

    @Test
    fun `the hoeffding bound is infinite at an empty leaf`() {
        // No margin can clear an infinite bound, which is what stops a leaf splitting on no data. The
        // guard also keeps the division below from producing an infinity by accident.
        assertEquals(Double.POSITIVE_INFINITY, hoeffdingBound(0.05, 0.0, depth = 0, decay = 0.9))
        assertEquals(Double.POSITIVE_INFINITY, hoeffdingBound(0.05, -1.0, depth = 0, decay = 0.9))
    }

    @Test
    fun `the hoeffding bound matches the closed form at depth zero`() {
        // decay^0 is 1, so at the root the bound is exactly sqrt(-ln(delta) / 2n) with no adjustment.
        val expected = sqrt(-ln(0.05) / (2.0 * 100.0))
        assertEquals(expected, hoeffdingBound(0.05, 100.0, depth = 0, decay = 0.9), DELTA)
    }

    @Test
    fun `the bound shrinks as evidence accumulates`() {
        // The property the whole split rule rests on: more observations means a tighter margin, so a
        // candidate that was not yet convincing becomes convincing without changing.
        var previous = Double.MAX_VALUE
        for (n in listOf(1.0, 10.0, 100.0, 1_000.0, 10_000.0)) {
            val bound = hoeffdingBound(0.05, n, depth = 0, decay = 0.9)
            assertTrue(bound < previous, "the bound did not shrink at n=$n: $previous then $bound")
            previous = bound
        }
    }

    @Test
    fun `the bound widens with depth`() {
        // The decay makes a deeper leaf demand a larger margin from the same amount of evidence, which
        // is the point: a deep leaf sees less of the stream, so equal confidence would be cheaper there.
        var previous = 0.0
        for (depth in 0..8) {
            val bound = hoeffdingBound(0.05, 100.0, depth = depth, decay = 0.9)
            assertTrue(bound > previous, "the bound did not widen at depth $depth: $previous then $bound")
            previous = bound
        }
    }

    @Test
    fun `a decay of one makes depth irrelevant`() {
        val root = hoeffdingBound(0.05, 100.0, depth = 0, decay = 1.0)
        for (depth in 1..5) {
            assertEquals(root, hoeffdingBound(0.05, 100.0, depth = depth, decay = 1.0), DELTA)
        }
    }

    @Test
    fun `a tighter confidence demands a wider margin`() {
        val loose = hoeffdingBound(0.5, 100.0, depth = 0, decay = 0.9)
        val tight = hoeffdingBound(0.001, 100.0, depth = 0, decay = 0.9)
        assertTrue(tight > loose, "a smaller delta should widen the bound: $tight vs $loose")
    }

    @Test
    fun `a null mtry keeps the whole pool without copying it`() {
        val pool = listOf("a", "b", "c")

        val picked = pool.pickCandidates(mtry = null, random = Random(0))

        // Identity, not just equality: the full-pool path is the common one and should not allocate.
        assertTrue(picked === pool, "the full pool was copied")
    }

    @Test
    fun `an mtry at least the pool size keeps the whole pool`() {
        val pool = listOf("a", "b", "c")

        assertTrue(pool.pickCandidates(mtry = 3, random = Random(0)) === pool)
        assertTrue(pool.pickCandidates(mtry = 99, random = Random(0)) === pool)
    }

    @Test
    fun `a smaller mtry draws that many distinct candidates from the pool`() {
        val pool = (0 until 20).toList()

        for (k in 1..19) {
            val picked = pool.pickCandidates(mtry = k, random = Random(k))
            assertEquals(k, picked.size, "wrong subspace size for mtry=$k")
            assertEquals(k, picked.toSet().size, "mtry=$k drew a duplicate: $picked")
            assertTrue(picked.all { it in pool }, "mtry=$k drew something outside the pool: $picked")
        }
    }

    @Test
    fun `the draw is reproducible from the seed and varies across seeds`() {
        // Both halves matter. Reproducibility is what makes a seeded forest testable at all; variation
        // is what actually decorrelates the trees, and a subspace draw that ignored the PRNG would
        // still pass every other test here.
        val pool = (0 until 20).toList()

        assertEquals(
            pool.pickCandidates(mtry = 5, random = Random(7)),
            pool.pickCandidates(mtry = 5, random = Random(7)),
            "the same seed drew a different subspace",
        )

        val draws = (0 until 8).map { pool.pickCandidates(mtry = 5, random = Random(it)) }.toSet()
        assertTrue(draws.size > 1, "every seed drew the same subspace: $draws")
    }

    @Test
    fun `an empty pool stays empty`() {
        val pool = emptyList<String>()
        assertEquals(pool, pool.pickCandidates(mtry = 3, random = Random(0)))
        assertEquals(pool, pool.pickCandidates(mtry = null, random = Random(0)))
    }

    @Test
    fun `both configs expose the shared tunables through one interface`() {
        // What makes the growth logic able to read tunables without knowing which tree it drives.
        val configs: List<HoeffdingTreeConfig> = listOf(RegressionTreeConfig(), ClassificationTreeConfig())
        for (config in configs) {
            assertEquals(0.05, config.delta, DELTA)
            assertEquals(0.9, config.deltaDecay, DELTA)
            assertEquals(0.05, config.tau, DELTA)
            assertEquals(30.0, config.minSamplesSplit, DELTA)
            assertEquals(5.0, config.minSamplesLeaf, DELTA)
            assertEquals(10, config.splitPeriod)
            assertEquals(16, config.maxDepth)
            assertEquals(1024, config.maxNodes)
            assertEquals(null, config.mtry)
        }
    }
}
