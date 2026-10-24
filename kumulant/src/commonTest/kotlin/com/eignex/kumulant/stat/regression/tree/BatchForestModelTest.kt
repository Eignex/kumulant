package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.feat
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BatchForestModelTest {
    @Test
    fun `bootstrap multiplicity and tree count do not fabricate independent noise evidence`() {
        val rows = listOf(RegressionObservation(feat(0.0), 2.0))
        for (trees in listOf(1, 10)) {
            val model = BatchRandomForest(1, BatchForestConfig(nbrTrees = trees)).fitModel(
                rows,
                Random(3),
                DenseVector.of(doubleArrayOf(4.0)),
            )
            val moments = model.predictMoments(feat(0.0))
            assertEquals(0.0, moments.disagreementVariance)
            assertEquals(4.0, moments.leafMeanVariance)
            assertEquals(4.0, moments.responseVariance)
        }
    }

    @Test
    fun `independent replicated observations reduce leaf mean uncertainty`() {
        val rows = List(4) { RegressionObservation(feat(0.0), 2.0) }
        val model = BatchRandomForest(1).fitModel(rows, Random(3), DenseVector.of(DoubleArray(4) { 4.0 }))
        assertEquals(1.0, model.predictMoments(feat(0.0)).leafMeanVariance)
        assertEquals(4.0, model.predictMoments(feat(0.0)).responseVariance)
        assertEquals(5.0, model.predictMoments(feat(0.0)).predictiveVariance)
    }

    @Test
    fun `sampled leaf offsets remain coherent under repeated and reordered queries`() {
        val model = BatchRandomForest(1, BatchForestConfig(bootstrap = false)).fitModel(
            listOf(RegressionObservation(feat(0.0), 1.0), RegressionObservation(feat(1.0), 3.0)),
            Random(3),
            DenseVector.of(doubleArrayOf(1.0, 1.0)),
        )
        val draw = model.drawFunction(Random(7))
        val left = draw.predict(feat(0.0))
        val right = draw.predict(feat(1.0))
        assertEquals(right, draw.predict(feat(1.0)))
        assertEquals(left, draw.predict(feat(0.0)))
        assertTrue(left != 1.0 && right != 3.0)
    }

    @Test
    fun `uncertainty model preserves existing fitted tree means`() {
        val rows = List(8) { RegressionObservation(feat(it.toDouble()), it.toDouble()) }
        val fitter = BatchRandomForest(1)
        val ordinary = fitter.fit(rows, Random(5))
        val model = fitter.fitModel(rows, Random(5))
        for (i in 0..8) {
            assertEquals(
                ordinary.predictDistribution(feat(i.toDouble())).mean,
                model.predictMoments(feat(i.toDouble())).mean,
            )
        }
    }
}
