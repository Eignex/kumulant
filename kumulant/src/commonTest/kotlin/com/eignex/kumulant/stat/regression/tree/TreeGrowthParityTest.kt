package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.core.F64VectorView
import com.eignex.kumulant.feat
import kotlin.test.Test
import kotlin.test.assertEquals

// Both trees are fed identical rows, with the regression target being the 0/1 class label as a Double.
// On binary labels the two split criteria are proportional (Bernoulli variance is exactly half the Gini
// impurity), so the candidate ranking is identical and both trees must reach the same structure.
class TreeGrowthParityTest {

    // Feature 0 decides the label for three quarters of the stream, feature 1 resolves the remaining
    // quarter, so the grown tree is a root split, one nested split, and three leaves.
    private val rows = listOf(
        feat(1.0, 0.0) to 1,
        feat(1.0, 0.0) to 1,
        feat(0.0, 0.0) to 0,
        feat(0.0, 1.0) to 1,
    )

    private val candidates = listOf(ThresholdSplit(0, 0.5), ThresholdSplit(1, 0.5))

    private fun regressionStructure(node: RegressionNode<F64VectorView>): String = when (node) {
        is RegressionSplitNode -> "(${node.split} ? ${regressionStructure(node.pos)}" +
            " : ${regressionStructure(node.neg)})"

        is RegressionLeafNode -> "leaf"
    }

    private fun classificationStructure(node: ClassificationNode): String = when (node) {
        is ClassificationSplitNode -> "(${node.split} ? ${classificationStructure(node.pos)}" +
            " : ${classificationStructure(node.neg)})"

        is ClassificationLeafNode -> "leaf"
    }

    private fun grownPair(observations: Int): Pair<RegressionTree<F64VectorView>, ClassificationTree> {
        val regression = RegressionTree<F64VectorView>(splitCandidates = candidates, randomSeed = 7)
        val classification = ClassificationTree(
            numClasses = 2,
            splitCandidates = candidates,
            randomSeed = 7,
        )
        repeat(observations) {
            val (row, label) = rows[it % rows.size]
            regression.update(row, label.toDouble())
            classification.update(row, label)
        }
        return regression to classification
    }

    @Test
    fun `both trees grow the same shape from the same stream`() {
        val (regression, classification) = grownPair(observations = 4000)

        assertEquals(5, regression.nodeCount, "regression shape:\n${regression.prettyPrint()}")
        assertEquals(
            regression.nodeCount,
            classification.nodeCount,
            "regression:\n${regression.prettyPrint()}\nclassification:\n${classification.prettyPrint()}",
        )
        assertEquals(
            regressionStructure(regression.rootNode()),
            classificationStructure(classification.rootNode()),
            "the two engines chose different splits",
        )
        assertEquals(4000.0, regression.rootSnapshot().totalWeights, 1e-9)
        assertEquals(4000.0, classification.rootSnapshot().totalWeights, 1e-9)
    }

    @Test
    fun `neither tree splits while the candidates carry no signal`() {
        val regression = RegressionTree<F64VectorView>(splitCandidates = candidates, randomSeed = 7)
        val classification = ClassificationTree(numClasses = 2, splitCandidates = candidates, randomSeed = 7)
        // A constant label leaves every candidate at zero impurity reduction, so shouldSplit must refuse
        // in both trees no matter how much weight piles up.
        repeat(500) {
            val row = rows[it % rows.size].first
            regression.update(row, 1.0)
            classification.update(row, 1)
        }
        assertEquals(1, regression.nodeCount)
        assertEquals(1, classification.nodeCount)
    }

    @Test
    fun `snapshot merge into a fresh tree reproduces the grown shape on both sides`() {
        val (regression, classification) = grownPair(observations = 4000)

        val freshRegression = RegressionTree<F64VectorView>(splitCandidates = candidates, randomSeed = 9)
        freshRegression.mergeSnapshot(regression.rootNode().snapshot())

        val freshClassification = ClassificationTree(numClasses = 2, splitCandidates = candidates, randomSeed = 9)
        freshClassification.mergeSnapshot(classification.rootNode().snapshot())

        assertEquals(regression.nodeCount, freshRegression.nodeCount)
        assertEquals(classification.nodeCount, freshClassification.nodeCount)
        assertEquals(
            regressionStructure(freshRegression.rootNode()),
            classificationStructure(freshClassification.rootNode()),
        )
        assertEquals(4000.0, freshRegression.rootSnapshot().totalWeights, 1e-9)
        assertEquals(4000.0, freshClassification.rootSnapshot().totalWeights, 1e-9)
    }
}
