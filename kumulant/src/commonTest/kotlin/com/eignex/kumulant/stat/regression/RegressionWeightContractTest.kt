package com.eignex.kumulant.stat.regression

import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorView
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

// The two halves are deliberately different in strength. A non-finite *weight* must be inert, because
// a weight is a multiplicity and an infinity is not one. A non-finite *value* only has to not throw: it
// may poison the model, because the observation really did arrive and really was unusable, and
// `filterFinite()` is how a caller declines to see such observations at all.
class RegressionWeightContractTest {

    // `snapshot` reads fields explicitly rather than stringifying the result: `ClassCountsResult` holds
    // a `DoubleArray`, whose `toString` is an identity hash, so a stringified tree snapshot would differ
    // from itself on every read.
    private class Model(
        val name: String,
        val y: Double,
        val update: (VectorView, Double, Double) -> Unit,
        val snapshot: () -> String,
    )

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
            Model("StochasticRegression", 2.0, { xv, yv, w -> stochastic.update(xv, yv, weight = w) }) {
                val r = stochastic.read()
                "${r.weights[0]},${r.weights[1]},${r.bias},${r.totalWeights},${r.step},${r.sse}"
            },
            Model("DiagonalRegression", 2.0, { xv, yv, w -> diagonal.update(xv, yv, weight = w) }) {
                val r = diagonal.read()
                "${r.weights[0]},${r.weights[1]},${r.bias},${r.totalWeights},${r.precision[0]},${r.sse}"
            },
            Model("BayesianRegression", 2.0, { xv, yv, w -> bayesian.update(xv, yv, weight = w) }) {
                val r = bayesian.read()
                "${r.weights[0]},${r.weights[1]},${r.bias},${r.totalWeights},${r.covariance[0, 0]},${r.sse}"
            },
            Model("SoftmaxRegression", 1.0, { xv, yv, w -> softmax.update(xv, yv, weight = w) }) {
                val r = softmax.read()
                val coefs = (0 until CLASSES).joinToString(",") { k ->
                    (0 until FEATURES).joinToString(",") { i -> "${r.weights[k, i]}" }
                }
                "$coefs,${r.biases[0]},${r.totalWeights},${r.step},${r.crossEntropy}"
            },
            Model("GaussianNaiveBayes", 1.0, { xv, yv, w -> gnb.update(xv, yv, weight = w) }) {
                val r = gnb.read()
                val moments = (0 until CLASSES).joinToString(",") { c ->
                    (0 until FEATURES).joinToString(",") { i -> "${r.means[c, i]},${r.variances[c, i]}" }
                }
                "$moments,${r.classWeights[0]},${r.classWeights[1]},${r.totalWeights}"
            },
            Model("DecisionTreeRegression", 2.0, { xv, yv, w -> dtReg.update(xv, yv, weight = w) }) {
                val r = dtReg.tree().rootSnapshot()
                "${r.totalWeights},${r.mean},${r.variance}"
            },
            Model("DecisionTreeClassifier", 1.0, { xv, yv, w -> dtClf.update(xv, yv, weight = w) }) {
                dtClf.tree().rootSnapshot().counts.joinToString(",")
            },
            Model("RandomForestRegression", 2.0, { xv, yv, w -> rfReg.update(xv, yv, weight = w) }) {
                rfReg.trees().joinToString(";") { t ->
                    val r = t.rootSnapshot()
                    "${r.totalWeights},${r.mean},${r.variance}"
                }
            },
            Model("RandomForestClassifier", 1.0, { xv, yv, w -> rfClf.update(xv, yv, weight = w) }) {
                rfClf.trees().joinToString(";") { it.rootSnapshot().counts.joinToString(",") }
            },
        )
    }

    @Test
    fun `an inert weight leaves every model untouched`() {
        // Zero, NaN, and both infinities. The infinities are the subtle ones: `+Infinity > 0.0` is
        // true, so a positivity test alone lets one through as an ordinary live observation.
        val inert = doubleArrayOf(0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)
        val violations = mutableListOf<String>()
        for (weight in inert) {
            for (model in models()) {
                model.update(x, model.y, 1.0)
                val before = model.snapshot()

                val thrown = runCatching { model.update(x, model.y, weight) }.exceptionOrNull()
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
        // A NaN in a weight vector is permanent: every later update multiplies it forward, so unlike a
        // bad quantile bucket there is no recovery and no way for the caller to tell the model is dead.
        val weights = doubleArrayOf(0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -1.0)
        val violations = mutableListOf<String>()
        for (weight in weights) {
            for (model in models()) {
                model.update(x, model.y, 1.0)
                runCatching { model.update(x, model.y, weight) }.exceptionOrNull()?.let { continue }

                val after = model.snapshot()
                if ("NaN" in after) violations += "${model.name} reports NaN after a weight of $weight"
                if ("Infinity" in after) violations += "${model.name} reports an infinity after $weight"

                // And it still trains, which a state comparison alone would not catch.
                model.update(x, model.y, 1.0)
                val recovered = model.snapshot()
                if ("NaN" in recovered || "Infinity" in recovered) {
                    violations += "${model.name} was poisoned by a weight of $weight"
                }
            }
        }
        assertEquals(emptyList(), violations.toList(), "no weight may put a non-finite number into a model")
    }

    @Test
    fun `a non-finite feature or target never throws`() {
        // A non-finite value is explicitly allowed to poison the model, but must not become an
        // exception. A caller who needs a stronger guarantee uses `filterFinite()`.
        val nonFinite = doubleArrayOf(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)
        val violations = mutableListOf<String>()
        for (bad in nonFinite) {
            val badFeatures = DenseVector.of(doubleArrayOf(bad, 1.0))
            for (model in models()) {
                // Feature position. The model is trained first so the bad input meets real state rather
                // than a fresh accumulator, which is where the arithmetic has more ways to go wrong.
                model.update(x, model.y, 1.0)
                runCatching { model.update(badFeatures, model.y, 1.0) }.exceptionOrNull()?.let {
                    violations += "${model.name} threw on a feature of $bad: ${it.message}"
                }
                runCatching { model.snapshot() }.exceptionOrNull()?.let {
                    violations += "${model.name} threw reading back after a feature of $bad: ${it.message}"
                }
            }
            for (model in models()) {
                // Target position, which reaches different arithmetic: a loss and a gradient rather than
                // a dot product. The classifiers treat it as a label, where asClassLabel already rejects
                // every non-finite value, so this confirms the rejection is silent rather than a throw.
                model.update(x, model.y, 1.0)
                runCatching { model.update(x, bad, 1.0) }.exceptionOrNull()?.let {
                    violations += "${model.name} threw on a target of $bad: ${it.message}"
                }
                runCatching { model.snapshot() }.exceptionOrNull()?.let {
                    violations += "${model.name} threw reading back after a target of $bad: ${it.message}"
                }
            }
        }
        assertEquals(emptyList(), violations.map { it.take(110) }, "a non-finite value must never throw")
    }

    @Test
    fun `a live weight still trains every model`() {
        // Guards the rules above: a model that ignored every weight would satisfy all of them, and so
        // would a snapshot that could not see the model change.
        for (model in models()) {
            val before = model.snapshot()

            model.update(x, model.y, 1.0)

            assertTrue(model.snapshot() != before, "${model.name} ignored a live observation")
        }
    }
}
