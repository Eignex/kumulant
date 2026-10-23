package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.feat
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class BatchRandomForestTest {
    @Test
    fun `CART learns an interaction requiring a zero gain first split`() {
        val rows = listOf(
            RegressionObservation(feat(0.0, 0.0), 0.0),
            RegressionObservation(feat(0.0, 1.0), 1.0),
            RegressionObservation(feat(1.0, 0.0), 1.0),
            RegressionObservation(feat(1.0, 1.0), 0.0),
        )
        val forest = BatchRandomForest(
            2,
            BatchForestConfig(nbrTrees = 1, bootstrap = false, mtry = 2),
        ).fit(rows, Random(3))
        for (row in rows) assertEquals(row.response, forest.predict(row.features), 1e-12)
    }

    @Test
    fun `numeric thresholds are fitted between observed continuous values`() {
        val forest = BatchRandomForest(1, BatchForestConfig(nbrTrees = 1, bootstrap = false)).fit(
            listOf(RegressionObservation(feat(0.31415), 1.0), RegressionObservation(feat(0.31416), 9.0)),
            Random(2),
        )
        val root = assertIs<TreeSplitResult>(forest.trees.single().root)
        val threshold = assertIs<ThresholdSplit>(root.split).threshold
        assertTrue(threshold > 0.31415 && threshold < 0.31416)
        assertEquals(1.0, forest.predict(feat(0.31415)))
        assertEquals(9.0, forest.predict(feat(0.31416)))
    }

    @Test
    fun `constant random subspaces search further for a valid partition`() {
        val rows = listOf(RegressionObservation(feat(0.0, 0.0), 1.0), RegressionObservation(feat(0.0, 1.0), 9.0))
        val fitter = BatchRandomForest(2, BatchForestConfig(nbrTrees = 1, mtry = 1, bootstrap = false))
        for (seed in 0 until 5) {
            val model = fitter.fit(rows, Random(seed))
            assertEquals(1.0, model.predict(feat(0.0, 0.0)))
            assertEquals(9.0, model.predict(feat(0.0, 1.0)))
        }
    }

    @Test
    fun `constant leaves respect statistical weights`() {
        val forest = BatchRandomForest(1, BatchForestConfig(nbrTrees = 1, maxDepth = 0, bootstrap = false)).fit(
            listOf(RegressionObservation(feat(0.0), 1.0), RegressionObservation(feat(0.0), 5.0, 3.0)),
        )
        assertEquals(4.0, forest.predict(feat(0.0)), 1e-12)
        assertEquals(4.0, forest.trees.single().totalWeights)
    }

    @Test
    fun `refitting replaces training data without mutating an earlier result`() {
        val fitter = BatchRandomForest(1, BatchForestConfig(nbrTrees = 1, bootstrap = false))
        val first = fitter.fit(listOf(RegressionObservation(feat(0.0), 1.0)))
        val second = fitter.fit(listOf(RegressionObservation(feat(0.0), 9.0)))
        assertEquals(1.0, first.predict(feat(0.0)))
        assertEquals(9.0, second.predict(feat(0.0)))
    }

    @Test
    fun `bootstrap fitting is reproducible with the same seed`() {
        val rows = List(12) { RegressionObservation(feat(it.toDouble()), it.toDouble()) }
        val fitter = BatchRandomForest(1)
        assertEquals(fitter.fit(rows, Random(9)), fitter.fit(rows, Random(9)))
    }

    @Test
    fun `bootstrap trees see different resampled histories`() {
        val rows = List(12) { RegressionObservation(feat(0.0), it.toDouble()) }
        val forest = BatchRandomForest(1, BatchForestConfig(maxDepth = 0)).fit(rows, Random(3))
        assertTrue(forest.trees.map { it.rootMean }.toSet().size > 1)
        for (tree in forest.trees) assertEquals(12.0, tree.totalWeights)
    }

    @Test
    fun `leaf weight constraints prevent unsupported splits`() {
        val forest = BatchRandomForest(1, BatchForestConfig(nbrTrees = 1, bootstrap = false)).fit(
            listOf(RegressionObservation(feat(0.0), 1.0, 0.2), RegressionObservation(feat(1.0), 9.0, 3.0)),
        )
        assertIs<TreeLeafResult>(forest.trees.single().root)
    }

    @Test
    fun `size bounds prevent tree growth`() {
        val rows = listOf(RegressionObservation(feat(0.0), 1.0), RegressionObservation(feat(1.0), 9.0))
        val configs = listOf(BatchForestConfig(maxDepth = 0), BatchForestConfig(maxNodes = 1))
        for (config in configs) {
            val forest = BatchRandomForest(1, config.copy(bootstrap = false)).fit(rows)
            for (tree in forest.trees) assertIs<TreeLeafResult>(tree.root)
        }
    }

    @Test
    fun `empty feature vectors fit a constant response model`() {
        val forest = BatchRandomForest(0, BatchForestConfig(nbrTrees = 1, bootstrap = false)).fit(
            listOf(RegressionObservation(feat(), 1.0), RegressionObservation(feat(), 3.0)),
        )
        assertEquals(2.0, forest.predict(feat()))
    }

    @Test
    fun `invalid feature data is rejected before fitting`() {
        val invalid = listOf(feat(0.0, 1.0), feat(Double.NaN), feat(Double.POSITIVE_INFINITY))
        for (features in invalid) {
            assertFailsWith<IllegalArgumentException> {
                BatchRandomForest(
                    1,
                ).fit(listOf(RegressionObservation(features, 1.0)))
            }
        }
    }
}
