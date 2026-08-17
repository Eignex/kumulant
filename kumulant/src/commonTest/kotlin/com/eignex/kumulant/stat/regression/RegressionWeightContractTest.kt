package com.eignex.kumulant.stat.regression

import com.eignex.koblas.DenseVector
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

private const val FEATURES = 2
private const val CLASSES = 2

/**
 * The four special weights, over the regression modality.
 *
 * The series, paired, and discrete modalities each have a contract sweep covering these. Regression had
 * none, which is how it stayed the modality where nobody had checked what an infinite weight does to a
 * model - and unlike a histogram bucket, a model absorbs a bad weight into state that no later
 * observation can clean.
 */
class RegressionWeightContractTest {

    /**
     * One model, with a way to train it and a way to read its learned numbers back.
     *
     * The snapshot reads fields explicitly rather than stringifying the result, and that is not
     * fastidiousness: `ClassCountsResult` holds a `DoubleArray`, whose `toString` is an identity hash, so
     * a stringified tree snapshot differs from itself on every read. An earlier version of this test did
     * exactly that and passed on the JVM while proving nothing. Kotlin/JS renders arrays differently and
     * failed honestly, which is the second time in this work that JS caught a vacuous JVM assertion.
     */
    private class Model(val name: String, val train: (Double) -> Unit, val snapshot: () -> String)

    private val splits = listOf(ThresholdSplit(featureIndex = 0, threshold = 0.5))
    private val x = DenseVector.of(DoubleArray(FEATURES) { 1.0 })

    // Bagging is off on the two forests. With it on, each tree draws Poisson(1) per observation and can
    // legitimately draw zero, so a single update may reach no tree at all. The early return that stops an
    // inert weight consuming a bagging draw has its own coverage in RandomForestRegressionStatTest.
    private fun models(): List<Model> {
        val stochastic = StochasticRegressionStat(FEATURES)
        val diagonal = DiagonalRegressionStat(FEATURES)
        val bayesian = BayesianRegressionStat(FEATURES)
        val softmax = SoftmaxRegressionStat(FEATURES, numClasses = CLASSES)
        val gnb = GaussianNaiveBayesStat(FEATURES, numClasses = CLASSES)
        val dtReg = DecisionTreeRegressionStat(FEATURES, splits)
        val dtClf = DecisionTreeClassifierStat(FEATURES, CLASSES, splits)
        val rfReg = RandomForestRegressionStat(FEATURES, splits, nbrTrees = 2, bagging = false)
        val rfClf = RandomForestClassifierStat(FEATURES, CLASSES, splits, nbrTrees = 2, bagging = false)
        return listOf(
            Model("StochasticRegression", { w -> stochastic.update(x, 2.0, weight = w) }) {
                val r = stochastic.read()
                "${r.weights[0]},${r.weights[1]},${r.bias},${r.totalWeights},${r.step},${r.sse}"
            },
            Model("DiagonalRegression", { w -> diagonal.update(x, 2.0, weight = w) }) {
                val r = diagonal.read()
                "${r.weights[0]},${r.weights[1]},${r.bias},${r.totalWeights},${r.precision[0]},${r.sse}"
            },
            Model("BayesianRegression", { w -> bayesian.update(x, 2.0, weight = w) }) {
                val r = bayesian.read()
                "${r.weights[0]},${r.weights[1]},${r.bias},${r.totalWeights},${r.covariance[0, 0]},${r.sse}"
            },
            Model("SoftmaxRegression", { w -> softmax.update(x, 1.0, weight = w) }) {
                val r = softmax.read()
                val coefs = (0 until CLASSES).joinToString(",") { k ->
                    (0 until FEATURES).joinToString(",") { i -> "${r.weights[k, i]}" }
                }
                "$coefs,${r.biases[0]},${r.totalWeights},${r.step},${r.crossEntropy}"
            },
            Model("GaussianNaiveBayes", { w -> gnb.update(x, 1.0, weight = w) }) {
                val r = gnb.read()
                val moments = (0 until CLASSES).joinToString(",") { c ->
                    (0 until FEATURES).joinToString(",") { i -> "${r.means[c, i]},${r.variances[c, i]}" }
                }
                "$moments,${r.classWeights[0]},${r.classWeights[1]},${r.totalWeights}"
            },
            Model("DecisionTreeRegression", { w -> dtReg.update(x, 2.0, weight = w) }) {
                val r = dtReg.tree().rootSnapshot()
                "${r.totalWeights},${r.mean},${r.variance}"
            },
            Model("DecisionTreeClassifier", { w -> dtClf.update(x, 1.0, weight = w) }) {
                dtClf.tree().rootSnapshot().counts.joinToString(",")
            },
            Model("RandomForestRegression", { w -> rfReg.update(x, 2.0, weight = w) }) {
                rfReg.trees().joinToString(";") { t ->
                    val r = t.rootSnapshot()
                    "${r.totalWeights},${r.mean},${r.variance}"
                }
            },
            Model("RandomForestClassifier", { w -> rfClf.update(x, 1.0, weight = w) }) {
                rfClf.trees().joinToString(";") { it.rootSnapshot().counts.joinToString(",") }
            },
        )
    }

    @Test
    fun `an inert weight leaves every model untouched`() {
        // Zero, NaN, and both infinities. The infinities are the ones that were unguarded:
        // `isInertWeight` tested only zero and NaN, and `+Infinity > 0.0` is true, so
        // `isNotPositiveWeight` let an infinite weight through as an ordinary live observation and into
        // the gradient step.
        val inert = doubleArrayOf(0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)
        val violations = mutableListOf<String>()
        for (weight in inert) {
            for (model in models()) {
                model.train(1.0)
                val before = model.snapshot()

                val thrown = runCatching { model.train(weight) }.exceptionOrNull()
                if (thrown != null) {
                    violations += "${model.name} threw on a weight of $weight: ${thrown.message}"
                    continue
                }

                if (model.snapshot() != before) violations += "${model.name} absorbed a weight of $weight"
            }
        }
        assertEquals(emptyList(), violations.map { it.take(110) }, "an inert weight must not train a model")
    }

    @Test
    fun `no weight puts a non-finite number into a model`() {
        // Stated in terms of what a caller sees, and the reason this modality matters most: a NaN in a
        // weight vector is permanent. Every later update multiplies it forward, so unlike a bad quantile
        // bucket there is no recovery and no way for the caller to tell the model is dead. An infinity is
        // no better, because it looks like a number a caller might trust and becomes NaN on the next
        // update via Infinity minus Infinity.
        val weights = doubleArrayOf(0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -1.0)
        val violations = mutableListOf<String>()
        for (weight in weights) {
            for (model in models()) {
                model.train(1.0)
                runCatching { model.train(weight) }.exceptionOrNull()?.let { continue }

                val after = model.snapshot()
                if ("NaN" in after) violations += "${model.name} reports NaN after a weight of $weight"
                if ("Infinity" in after) violations += "${model.name} reports an infinity after $weight"

                // And it still trains, which a state comparison alone would not catch.
                model.train(1.0)
                val recovered = model.snapshot()
                if ("NaN" in recovered || "Infinity" in recovered) {
                    violations += "${model.name} was poisoned by a weight of $weight"
                }
            }
        }
        assertEquals(emptyList(), violations.toList(), "no weight may put a non-finite number into a model")
    }

    @Test
    fun `a live weight still trains every model`() {
        // Guards the rules above: a model that ignored every weight would satisfy all of them, and so
        // would a snapshot that could not see the model change.
        for (model in models()) {
            val before = model.snapshot()

            model.train(1.0)

            assertTrue(model.snapshot() != before, "${model.name} ignored a live observation")
        }
    }
}
