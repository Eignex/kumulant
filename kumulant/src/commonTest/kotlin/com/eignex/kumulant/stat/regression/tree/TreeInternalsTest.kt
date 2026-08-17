package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.DELTA
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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

    // A leaf payload standing in for both real ones, so the ranking tests exercise the shared loop
    // rather than either metric's arithmetic.
    private fun counts(vararg c: Double) = ClassCountsResult(c.size, c)

    @Test
    fun `ranking picks the best candidate and remembers the runner-up`() {
        // The runner-up is what the Hoeffding test consumes, so losing track of it would not fail any
        // "did it pick the best one" assertion while breaking the split rule completely.
        val total = counts(50.0, 50.0)
        val pos = listOf(counts(40.0, 10.0), counts(30.0, 20.0), counts(26.0, 24.0))
        val neg = listOf(counts(10.0, 40.0), counts(20.0, 30.0), counts(24.0, 26.0))

        val ranked = rankCandidates(total, pos, neg, minSamplesSplit = 30.0, minSamplesLeaf = 5.0) { t, p, n ->
            GiniReduction.score(t, p, n)
        }

        assertEquals(0, ranked.bestIndex, "the most separating candidate should win")
        assertTrue(ranked.top1 > ranked.top2, "the runner-up should score below the winner")
        assertTrue(ranked.top2 > 0.0, "the second candidate separates too, so top2 should not be zero")
    }

    @Test
    fun `a candidate too thin on one side is skipped rather than scored`() {
        // A split isolating three observations can score beautifully and predict nothing, so the
        // thinness gate has to run before the score is even considered - not after, as a tiebreak.
        val total = counts(50.0, 50.0)
        val perfectButThin = counts(3.0, 0.0)
        val theRest = counts(47.0, 50.0)

        val ranked = rankCandidates(
            total,
            listOf(perfectButThin),
            listOf(theRest),
            minSamplesSplit = 30.0,
            minSamplesLeaf = 5.0,
        ) { t, p, n -> GiniReduction.score(t, p, n) }

        assertEquals(-1, ranked.bestIndex, "a candidate with 3 observations on one side was accepted")
        assertEquals(0.0, ranked.top1, DELTA)
    }

    @Test
    fun `mismatched candidate lists are rejected`() {
        val total = counts(10.0, 10.0)
        val e = runCatching {
            rankCandidates(total, listOf(counts(5.0, 5.0)), emptyList(), 1.0, 1.0) { _, _, _ -> 1.0 }
        }.exceptionOrNull()
        assertTrue(e is IllegalArgumentException, "expected a rejection, got $e")
    }

    @Test
    fun `a leaf below minSamplesSplit does not split`() {
        val ranked = SplitInfo(top1 = 10.0, top2 = 0.0, bestIndex = 0)
        val config = ClassificationTreeConfig(minSamplesSplit = 30.0)

        assertEquals(false, shouldSplit(ranked, totalWeight = 29.0, depth = 0, config = config))
        assertEquals(true, shouldSplit(ranked, totalWeight = 30.0, depth = 0, config = config))
    }

    @Test
    fun `a leaf with no qualifying candidate does not split`() {
        val config = ClassificationTreeConfig()
        assertEquals(false, shouldSplit(SplitInfo(0.0, 0.0, -1), 1000.0, 0, config))
        // Something scored, but no better than not splitting; every metric returns 0 for no signal.
        assertEquals(false, shouldSplit(SplitInfo(0.0, 0.0, 2), 1000.0, 0, config))
    }

    @Test
    fun `the hoeffding test needs the winner to clear the runner-up by the bound`() {
        val config = ClassificationTreeConfig(tau = 0.0) // tau off, so only the Hoeffding rule applies
        val bound = hoeffdingBound(config.delta, 1000.0, 0, config.deltaDecay)

        val clears = SplitInfo(top1 = bound * 3.0, top2 = 0.0, bestIndex = 0)
        val doesNot = SplitInfo(top1 = bound * 3.0, top2 = bound * 3.0 - bound / 2.0, bestIndex = 0)

        assertTrue(shouldSplit(clears, 1000.0, 0, config), "a clear winner should split")
        assertTrue(
            !shouldSplit(doesNot, 1000.0, 0, config),
            "a winner inside the bound of the runner-up should wait",
        )
    }

    @Test
    fun `the tau tie-break breaks a deadlock the hoeffding rule cannot`() {
        // Two candidates of equal merit never separate by more than any bound, so without tau the leaf
        // grows forever without splitting. This is the VFDT tie-break, and it is the reason the
        // decision is a disjunction rather than a single test.
        val tied = SplitInfo(top1 = 5.0, top2 = 5.0, bestIndex = 0)
        val bigLeaf = 1_000_000.0

        val withoutTau = ClassificationTreeConfig(tau = 0.0)
        assertTrue(!shouldSplit(tied, bigLeaf, 0, withoutTau), "with tau off, a tie must never split")

        val withTau = ClassificationTreeConfig(tau = 0.05)
        assertTrue(shouldSplit(tied, bigLeaf, 0, withTau), "tau should let a well-evidenced tie split")

        // And tau is not a blanket override: the same tie on a small leaf still waits, because the
        // bound has not shrunk below tau yet.
        assertTrue(!shouldSplit(tied, 40.0, 0, withTau), "tau should not fire before the bound shrinks")
    }

    @Test
    fun `both trees reach the same decision from the same evidence`() {
        // Identical tunables in both config types must produce identical verdicts.
        val cases = listOf(
            SplitInfo(10.0, 0.0, 0),
            SplitInfo(0.0, 0.0, -1),
            SplitInfo(5.0, 5.0, 0),
            SplitInfo(0.001, 0.0005, 1),
        )
        for (weight in listOf(29.0, 100.0, 100_000.0)) {
            for (depth in listOf(0, 4, 12)) {
                for (ranked in cases) {
                    assertEquals(
                        shouldSplit(ranked, weight, depth, RegressionTreeConfig()),
                        shouldSplit(ranked, weight, depth, ClassificationTreeConfig()),
                        "the two trees disagreed at weight=$weight depth=$depth on $ranked",
                    )
                }
            }
        }
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
