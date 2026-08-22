package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.core.F64DenseVector
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DecisionTreeClassifierStatTest {

    private val splits = listOf(
        ThresholdSplit(featureIndex = 0, threshold = 0.0),
        ThresholdSplit(featureIndex = 1, threshold = 0.0),
    )

    @Test
    fun `learns an axis-aligned binary partition`() {
        val stat = DecisionTreeClassifierStat(
            featureSize = 2,
            numClasses = 2,
            splitCandidates = splits,
            config = ClassificationTreeConfig(
                splitPeriod = 5,
                minSamplesSplit = 10.0,
                minSamplesLeaf = 2.0,
                delta = 0.5,
                tau = 0.5,
            ),
        )
        val rng = Random(1L)
        repeat(800) {
            val x0 = rng.nextDouble() * 2.0 - 1.0
            val x1 = rng.nextDouble() * 2.0 - 1.0
            val label = if (x0 > 0.0) 1 else 0
            stat.update(doubleArrayOf(x0, x1), label.toDouble())
        }
        val r = stat.read()
        val nRight = (0 until 200).count {
            val x0 = rng.nextDouble() * 2.0 - 1.0
            val x1 = rng.nextDouble() * 2.0 - 1.0
            val label = if (x0 > 0.0) 1 else 0
            r.predict(F64DenseVector.of(doubleArrayOf(x0, x1))) == label
        }
        assertTrue(nRight > 180, "accuracy=$nRight/200, tree=${stat.tree().prettyPrint()}")
    }

    @Test
    fun `out of range class labels are dropped`() {
        val stat = DecisionTreeClassifierStat(featureSize = 1, numClasses = 2, splitCandidates = splits.take(1))
        stat.update(doubleArrayOf(0.5), 7.0)
        stat.update(doubleArrayOf(0.5), -1.0)
        assertEquals(0.0, stat.read().totalWeights, 1e-9)
    }

    @Test
    fun `probabilities sum to one at every leaf`() {
        val stat = DecisionTreeClassifierStat(featureSize = 2, numClasses = 3, splitCandidates = splits)
        val rng = Random(2L)
        repeat(200) {
            val x0 = rng.nextDouble() * 2.0 - 1.0
            val x1 = rng.nextDouble() * 2.0 - 1.0
            stat.update(doubleArrayOf(x0, x1), rng.nextInt(3).toDouble())
        }
        val r = stat.read()
        val p = r.probabilities(F64DenseVector.of(doubleArrayOf(0.4, -0.4)))
        assertEquals(1.0, p.sum(), 1e-9)
    }

    @Test
    fun `reset zeroes accumulated weight`() {
        val stat = DecisionTreeClassifierStat(featureSize = 1, numClasses = 2, splitCandidates = splits.take(1))
        repeat(20) { stat.update(doubleArrayOf(0.5), 1.0) }
        stat.reset()
        assertEquals(0.0, stat.read().totalWeights, 1e-9)
    }

    @Test
    fun `merge folds another snapshot into the running tree`() {
        val a = DecisionTreeClassifierStat(featureSize = 1, numClasses = 2, splitCandidates = splits.take(1))
        val b = DecisionTreeClassifierStat(featureSize = 1, numClasses = 2, splitCandidates = splits.take(1))
        repeat(10) { a.update(doubleArrayOf(0.5), 0.0) }
        repeat(10) { b.update(doubleArrayOf(-0.5), 1.0) }
        a.merge(b.read())
        assertEquals(20.0, a.read().totalWeights, 1e-9)
    }
}
