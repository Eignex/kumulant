package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.math.DenseVector
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RandomForestClassifierStatTest {

    private val splits = listOf(
        ThresholdSplit(0, 0.0),
        ThresholdSplit(1, 0.0),
    )

    @Test
    fun `learns a separable problem and probabilities sum to one`() {
        val stat = RandomForestClassifierStat(
            featureSize = 2,
            numClasses = 2,
            splitCandidates = splits,
            nbrTrees = 5,
            config = ClassificationTreeConfig(
                splitPeriod = 5,
                minSamplesSplit = 10.0,
                minSamplesLeaf = 2.0,
                delta = 0.5,
                tau = 0.5,
            ),
            randomSeed = 7,
        )
        val rng = Random(3L)
        repeat(800) {
            val x0 = rng.nextDouble() * 2.0 - 1.0
            val x1 = rng.nextDouble() * 2.0 - 1.0
            val label = if (x0 > 0.0) 1 else 0
            stat.update(doubleArrayOf(x0, x1), label.toDouble())
        }
        val r = stat.read()
        val p = r.probabilities(DenseVector.of(doubleArrayOf(0.5, 0.5)))
        assertEquals(1.0, p.sum(), 1e-9)
        val correct = (0 until 200).count {
            val x0 = rng.nextDouble() * 2.0 - 1.0
            val x1 = rng.nextDouble() * 2.0 - 1.0
            val label = if (x0 > 0.0) 1 else 0
            r.predict(DenseVector.of(doubleArrayOf(x0, x1))) == label
        }
        assertTrue(correct > 170, "accuracy=$correct/200")
    }

    @Test
    fun `merge requires matching forest size and numClasses`() {
        val a = RandomForestClassifierStat(
            featureSize = 1,
            numClasses = 2,
            splitCandidates = splits.take(1),
            nbrTrees = 3,
        )
        val b = RandomForestClassifierStat(
            featureSize = 1,
            numClasses = 2,
            splitCandidates = splits.take(1),
            nbrTrees = 3,
        )
        repeat(5) { a.update(doubleArrayOf(0.5), 1.0) }
        repeat(5) { b.update(doubleArrayOf(-0.5), 0.0) }
        a.merge(b.read())
        val r = a.read()
        assertTrue(r.totalWeights > 0.0)
        assertEquals(2, r.numClasses)
    }
}
