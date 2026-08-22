package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64DenseVector
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeClassifierStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.regression.tree.RandomForestClassifierStat
import com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat
import com.eignex.kumulant.stat.regression.tree.ThresholdSplit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val FEATURES = 3

// A wrong-arity vector is the one input error that does not announce itself: a *short* vector reads
// fewer coordinates and returns a perfectly plausible number from the wrong model. Swept across the
// catalogue so a stat added later cannot go missing its guard.
class RegressionArityGuardTest {

    private val splits = listOf(ThresholdSplit(featureIndex = 0, threshold = 0.5))

    private val stats: List<Pair<String, RegressionStat<*>>> = listOf(
        "StochasticRegression" to StochasticRegressionStat(featureSize = FEATURES),
        "DiagonalRegression" to DiagonalRegressionStat(featureSize = FEATURES),
        "BayesianRegression" to BayesianRegressionStat(featureSize = FEATURES),
        "SoftmaxRegression" to SoftmaxRegressionStat(featureSize = FEATURES, numClasses = 2),
        "GaussianNaiveBayes" to GaussianNaiveBayesStat(featureSize = FEATURES, numClasses = 2),
        "DecisionTreeRegression" to DecisionTreeRegressionStat(FEATURES, splits),
        "DecisionTreeClassifier" to DecisionTreeClassifierStat(FEATURES, numClasses = 2, splitCandidates = splits),
        "RandomForestRegression" to RandomForestRegressionStat(FEATURES, splits, nbrTrees = 2),
        "RandomForestClassifier" to RandomForestClassifierStat(FEATURES, numClasses = 2, splitCandidates = splits),
    )

    @Test
    fun `every regression stat rejects a wrong-arity context vector`() {
        // Short and long both, because the two fail differently: a long vector would at worst read
        // past what the model uses, but a short one is the silent case the guard exists for.
        val violations = mutableListOf<String>()
        for ((name, stat) in stats) {
            for (size in listOf(FEATURES - 1, FEATURES + 1)) {
                val x = F64DenseVector.of(DoubleArray(size) { 1.0 })
                val thrown = runCatching { stat.update(x, 1.0) }.exceptionOrNull()
                when {
                    thrown == null -> violations += "$name accepted a vector of size $size"

                    thrown !is IllegalArgumentException ->
                        violations += "$name threw ${thrown::class.simpleName} rather than IllegalArgumentException"
                }
            }
        }
        assertEquals(emptyList(), violations.toList(), "a wrong-arity vector must be rejected")
    }

    @Test
    fun `the rejection names both the size it got and the size it wanted`() {
        // Pinned because the message is the only thing that tells a caller which of the two numbers to
        // change, and because RegressionOpsTest greps it.
        for ((name, stat) in stats) {
            val thrown = runCatching {
                stat.update(F64DenseVector.of(doubleArrayOf(1.0)), 1.0)
            }.exceptionOrNull()
            val message = thrown?.message.orEmpty()
            assertTrue("x.size=1" in message, "$name did not report the size it got: $message")
            assertTrue("expected $FEATURES" in message, "$name did not report the size it wanted: $message")
        }
    }

    @Test
    fun `a correctly sized vector is still accepted`() {
        // Guards the guard: a require that rejected everything would satisfy both tests above.
        for ((name, stat) in stats) {
            val x = F64DenseVector.of(DoubleArray(FEATURES) { 1.0 })
            val thrown = runCatching { stat.update(x, 1.0) }.exceptionOrNull()
            assertEquals(null, thrown?.message, "$name rejected a correctly sized vector")
        }
    }
}
