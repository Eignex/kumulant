package com.eignex.kumulant.stat.regression

import com.eignex.koblas.F64DenseMatrix
import com.eignex.koblas.F64DenseVector
import com.eignex.kumulant.stat.anomaly.FeatureRange
import com.eignex.kumulant.stat.anomaly.HalfSpaceTreesStat
import com.eignex.kumulant.stat.regression.glm.CovarianceRegressionResult
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat
import com.eignex.kumulant.stat.regression.tree.ThresholdSplit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// A model must not silently get *worse* by folding in a snapshot that carries no information, and
// must not train on a label that is not one.
class RegressionMergeGuardTest {

    private val splits = listOf(ThresholdSplit(0, 4.5))

    @Test
    fun `merging untrained snapshots leaves a trained diagonal fit alone`() {
        val stat = DiagonalRegressionStat(featureSize = 1)
        repeat(5) { i -> stat.update(doubleArrayOf(i.toDouble()), 3.0 * i) }
        val before = stat.read()

        repeat(20) { stat.merge(DiagonalRegressionStat(featureSize = 1).read()) }

        val after = stat.read()
        // Every replica seeds its precision at priorPrecision, so an additive pool would count the
        // prior once per merge while folding in snapshots that contain no data at all.
        assertEquals(before.weights[0], after.weights[0], 1e-9, "an empty snapshot moved the weights")
        assertEquals(before.precision[0], after.precision[0], 1e-9, "an empty snapshot moved the precision")
        assertEquals(before.totalWeights, after.totalWeights, 1e-9)
    }

    @Test
    fun `merging two trained diagonal replicas counts the prior once`() {
        val a = DiagonalRegressionStat(featureSize = 1)
        val b = DiagonalRegressionStat(featureSize = 1)
        repeat(5) { i ->
            a.update(doubleArrayOf(i.toDouble()), 3.0 * i)
            b.update(doubleArrayOf(i.toDouble()), 3.0 * i)
        }
        val single = a.read().precision[0]

        a.merge(b.read())

        // Two replicas of the same evidence: the pooled precision is 2*(single - prior) + prior.
        val prior = 1.0
        assertEquals(2.0 * (single - prior) + prior, a.read().precision[0], 1e-9)
    }

    @Test
    fun `a merged half-space-trees model can score immediately`() {
        val ranges = List(1) { FeatureRange(0.0, 1.0) }
        val source = HalfSpaceTreesStat(featureSize = 1, featureRanges = ranges, windowSize = 50)
        repeat(200) { source.update(doubleArrayOf(0.5)) }
        val sourceScore = source.read().score(F64DenseVector.of(doubleArrayOf(0.5)))

        val target = source.create()
        target.merge(source.read())

        // The reference window is what score() reads, so folding into the latest window alone would
        // leave a merged model scoring every input maximally anomalous.
        val mergedScore = target.read().score(F64DenseVector.of(doubleArrayOf(0.5)))
        assertTrue(mergedScore > 0.0, "a merged model scored $mergedScore, i.e. maximally anomalous")
        assertEquals(sourceScore, mergedScore, sourceScore * 1e-9)
    }

    @Test
    fun `an absurd half-space-trees height is rejected at construction`() {
        val ranges = List(1) { FeatureRange(0.0, 1.0) }
        // 31 wraps `1 shl height` negative and 32 wraps it to 1, so neither can allocate a node array.
        assertFailsWith<IllegalArgumentException> {
            HalfSpaceTreesStat(featureSize = 1, featureRanges = ranges, height = 31)
        }
        assertFailsWith<IllegalArgumentException> {
            HalfSpaceTreesStat(featureSize = 1, featureRanges = ranges, height = 32)
        }
    }

    @Test
    fun `a NaN or fractional label does not train class zero`() {
        val gnb = GaussianNaiveBayesStat(featureSize = 1, numClasses = 2)
        val softmax = SoftmaxRegressionStat(featureSize = 1, numClasses = 2)

        gnb.update(doubleArrayOf(100.0), Double.NaN)
        gnb.update(doubleArrayOf(100.0), -0.7)
        softmax.update(doubleArrayOf(100.0), Double.NaN)
        softmax.update(doubleArrayOf(100.0), -0.7)

        // toInt() truncates toward zero, so an unguarded label would land in class 0 and pass the
        // range check.
        assertEquals(0.0, gnb.read().totalWeights, "GaussianNaiveBayes trained on a non-label")
        assertEquals(0.0, softmax.read().totalWeights, "SoftmaxRegression trained on a non-label")
    }

    @Test
    fun `a zero-weight update leaves a forest and a tree unchanged`() {
        val clean = RandomForestRegressionStat(featureSize = 1, splitCandidates = splits, nbrTrees = 4)
        val probed = RandomForestRegressionStat(featureSize = 1, splitCandidates = splits, nbrTrees = 4)
        for (i in 0 until 200) {
            val x = doubleArrayOf((i % 10).toDouble())
            val y = 2.0 * (i % 10)
            probed.update(x, y, 0.0) // must consume no bagging draws
            clean.update(x, y, 1.0)
            probed.update(x, y, 1.0)
        }

        // A zero-weight call that drew once per tree from baggingRng would desynchronise every later
        // draw and change the forest's predictions.
        val at = F64DenseVector.of(doubleArrayOf(3.0))
        assertEquals(clean.read().predict(at), probed.read().predict(at), 1e-12)

        val tree = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = splits)
        val treeClean = DecisionTreeRegressionStat(featureSize = 1, splitCandidates = splits)
        for (i in 0 until 100) {
            val x = doubleArrayOf((i % 10).toDouble())
            tree.update(x, 2.0 * (i % 10), 0.0)
            tree.update(x, 2.0 * (i % 10), 1.0)
            treeClean.update(x, 2.0 * (i % 10), 1.0)
        }
        assertEquals(treeClean.read().predict(at), tree.read().predict(at), 1e-12)
    }

    @Test
    fun `a non-square covariance is rejected by name`() {
        val error = assertFailsWith<IllegalArgumentException> {
            CovarianceRegressionResult(
                weights = F64DenseVector.of(doubleArrayOf(1.0, 2.0)),
                bias = 0.0,
                biasPrecision = 1.0,
                totalWeights = 1.0,
                step = 1L,
                covariance = F64DenseMatrix.zero(2, 3),
                covarianceL = F64DenseMatrix.zero(2, 2),
            )
        }
        assertTrue(error.message?.contains("2x3") == true, "expected the shape in the message, got ${error.message}")
    }
}
