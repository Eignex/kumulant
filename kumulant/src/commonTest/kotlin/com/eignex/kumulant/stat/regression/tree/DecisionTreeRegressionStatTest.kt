package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.Workspace
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.feat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class DecisionTreeRegressionStatTest {

    private class RecordingLeafArm : SeriesStat<WeightedVarianceResult> {
        private val delegate = VarianceStat()
        override val concurrency = delegate.concurrency
        var workspace: Workspace? = null

        override fun update(value: Double, timestampNanos: Long, weight: Double) {
            delegate.update(value, timestampNanos, weight)
        }

        override fun merge(values: WeightedVarianceResult, workspace: Workspace?) {
            this.workspace = workspace
            delegate.merge(values, workspace)
        }

        override fun reset() = delegate.reset()

        override fun read(timestampNanos: Long) = delegate.read(timestampNanos)

        override fun create(concurrency: Concurrency?) = RecordingLeafArm()
    }

    @Test
    fun `tree merge forwards workspace to leaf arm`() {
        val leafArms = mutableListOf<RecordingLeafArm>()
        val target = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = emptyList(),
            leafArmFactory = { RecordingLeafArm().also(leafArms::add) },
        )
        val source = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList())
        source.update(feat(0.0), 1.0)
        val workspace = Workspace()

        target.merge(source.read(0L), workspace)

        assertSame(workspace, leafArms.single().workspace)
    }

    @Test
    fun `tree merge forwards workspace while cloning a snapshot subtree`() {
        val config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0)
        val source = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = config,
            randomSeed = 83,
        )
        repeat(20) {
            val x = if (it % 2 == 0) -1.0 else 1.0
            source.update(feat(x), x)
        }
        val leafArms = mutableListOf<RecordingLeafArm>()
        val target = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = config,
            leafArmFactory = { RecordingLeafArm().also(leafArms::add) },
            randomSeed = 84,
        )
        target.update(feat(0.0), 1.0)
        val workspace = Workspace()

        target.merge(source.read(0L), workspace)

        val mergedArms = leafArms.filter { it.workspace != null }
        assertTrue(mergedArms.size >= 3)
        for (arm in mergedArms) assertSame(workspace, arm.workspace)
    }

    @Test
    fun `rejects bad featureSize`() {
        assertFailsWith<IllegalArgumentException> {
            DecisionTreeRegressionStat(featureSize = 0, splitCandidates = emptyList())
        }
    }

    @Test
    fun `predict reflects context routing after splits grow`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
            randomSeed = 1,
        )
        val rng = Random(1)
        repeat(200) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), if (x > 0) 1.0 else -1.0)
        }
        val snap = stat.read(0L)
        assertTrue(snap.predict(feat(0.5)) > snap.predict(feat(-0.5)))
    }

    @Test
    fun `merge folds snapshot into stat`() {
        val a = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 2)
        val b = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 3)
        repeat(20) {
            a.update(feat(0.0), 1.0)
            b.update(feat(0.0), 3.0)
        }
        val aBefore = a.read(0L).totalWeights
        a.merge(b.read(0L))
        assertTrue(a.read(0L).totalWeights > aBefore)
    }

    @Test
    fun `reset returns to a single leaf`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 4,
        )
        repeat(100) { stat.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        assertTrue(stat.tree().nodeCount >= 3)
        stat.reset()
        assertEquals(1, stat.tree().nodeCount)
    }

    @Test
    fun `empty splitCandidates degenerates to a single leaf`() {
        val stat = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = emptyList(), randomSeed = 40)
        repeat(500) { stat.update(feat(it.toDouble()), it.toDouble()) }
        assertEquals(1, stat.tree().nodeCount)
        val snap = stat.read(0L)
        assertEquals(snap.predict(feat(-100.0)), snap.predict(feat(100.0)))
    }

    @Test
    fun `maxNodes caps growth`() {
        val candidates = (0 until 8).map { ThresholdSplit(0, it * 0.2 - 0.8) }
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = candidates,
            config = RegressionTreeConfig(
                splitPeriod = 4,
                minSamplesSplit = 4.0,
                minSamplesLeaf = 1.0,
                maxNodes = 5,
                tau = 1.0,
            ),
            randomSeed = 41,
        )
        val rng = Random(41)
        repeat(2000) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), x)
        }
        assertTrue(stat.tree().nodeCount <= 5, "nodeCount=${stat.tree().nodeCount}")
    }

    @Test
    fun `maxDepth caps growth`() {
        val candidates = (0 until 8).map { ThresholdSplit(0, it * 0.2 - 0.8) }
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = candidates,
            config = RegressionTreeConfig(
                splitPeriod = 4,
                minSamplesSplit = 4.0,
                minSamplesLeaf = 1.0,
                maxDepth = 2,
                tau = 1.0,
            ),
            randomSeed = 42,
        )
        val rng = Random(42)
        repeat(2000) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), x)
        }
        assertTrue(stat.tree().nodeCount <= 7, "nodeCount=${stat.tree().nodeCount}")
    }

    @Test
    fun `prettyPrint renders split structure`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 50,
        )
        repeat(200) { stat.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        val rendered = stat.tree().prettyPrint()
        assertTrue("x[0]" in rendered, "expected split predicate, got:\n$rendered")
        assertTrue("leaf mean=" in rendered)
        assertTrue("} else {" in rendered)
    }

    @Test
    fun `snapshot routes to same leaf as live tree`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0),
            randomSeed = 60,
        )
        val rng = Random(60)
        repeat(300) {
            val x = rng.nextDouble() * 2 - 1
            stat.update(feat(x), x)
        }
        val snap = stat.read(0L)
        for (x in listOf(-0.9, -0.1, 0.1, 0.9)) {
            assertEquals(stat.tree().predict(feat(x)), snap.predict(feat(x)), 1e-12)
        }
    }

    @Test
    fun `Concurrency Relaxed preserves total weight on serial updates`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
            concurrency = Concurrency.Relaxed,
            randomSeed = 70,
        )
        var expected = 0.0
        val rng = Random(70)
        repeat(500) {
            val w = 1.0 + rng.nextDouble()
            stat.update(feat(rng.nextDouble() * 2 - 1), rng.nextDouble(), weight = w)
            expected += w
        }
        assertEquals(expected, stat.read(0L).totalWeights, 1e-9)
    }

    @Test
    fun `Concurrency Strict preserves total weight on serial updates`() {
        val stat = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 8, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
            concurrency = Concurrency.Strict,
            randomSeed = 71,
        )
        var expected = 0.0
        val rng = Random(71)
        repeat(500) {
            val w = 1.0 + rng.nextDouble()
            stat.update(feat(rng.nextDouble() * 2 - 1), rng.nextDouble(), weight = w)
            expected += w
        }
        assertEquals(expected, stat.read(0L).totalWeights, 1e-9)
    }

    @Test
    fun `create rebuilds leaf arms at the requested concurrency`() {
        val stat = DecisionTreeRegressionStat(featureSize = 2, splitCandidates = emptyList())
        val replica = stat.create(Concurrency.Strict)
        val leaf = replica.tree().rootNode() as RegressionLeafNode
        assertEquals(Concurrency.Strict, leaf.arm.concurrency)
    }

    @Test
    fun `a tree merged from a snapshot can still grow`() {
        val candidates = listOf(ThresholdSplit(0, -0.5), ThresholdSplit(0, 0.0), ThresholdSplit(0, 0.5))
        fun newStat(seed: Int) = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = candidates,
            config = RegressionTreeConfig(splitPeriod = 4, minSamplesSplit = 4.0, minSamplesLeaf = 2.0, tau = 1.0),
            randomSeed = seed,
        )
        val xs = listOf(-1.0, -0.2, 0.2, 1.0)
        fun drive(stat: DecisionTreeRegressionStat, n: Int) {
            repeat(n) {
                val x = xs[it % xs.size]
                stat.update(feat(x), x)
            }
        }

        val worker = newStat(4)
        drive(worker, 8)
        val accumulator = newStat(5)
        accumulator.merge(worker.read(0L))
        val afterMerge = accumulator.tree().nodeCount
        drive(accumulator, 400)

        val control = newStat(5)
        drive(control, 400)
        assertTrue(control.tree().nodeCount > 1, "control tree never grew, the fixture cannot detect a freeze")
        assertTrue(
            accumulator.tree().nodeCount > afterMerge,
            "node count stuck at $afterMerge after merging a snapshot",
        )
    }

    @Test
    fun `merged mass does not tighten the hoeffding bound`() {
        // Two candidates that split the stream equally well, so the margin is zero and the decision
        // rests entirely on the bound. tau sits between the bound at 10 observations (0.387) and at
        // 20 (0.274), so counting the donor's merged mass as evidence is the only way to clear it.
        val candidates = listOf(ThresholdSplit(0, 0.0), ThresholdSplit(0, 0.5))
        fun newStat(seed: Int) = DecisionTreeRegressionStat(
            featureSize = 1,
            splitCandidates = candidates,
            config = RegressionTreeConfig(
                splitPeriod = 10,
                minSamplesSplit = 8.0,
                minSamplesLeaf = 2.0,
                tau = 0.30,
            ),
            randomSeed = seed,
        )
        fun drive(stat: DecisionTreeRegressionStat, n: Int) {
            repeat(n) { stat.update(feat(if (it % 2 == 0) -1.0 else 1.0), if (it % 2 == 0) -1.0 else 1.0) }
        }

        val donor = newStat(11)
        drive(donor, 10)
        val merged = newStat(12)
        merged.merge(donor.read(0L))
        drive(merged, 10)

        val control = newStat(12)
        drive(control, 10)
        assertEquals(control.tree().nodeCount, merged.tree().nodeCount)
    }
}
