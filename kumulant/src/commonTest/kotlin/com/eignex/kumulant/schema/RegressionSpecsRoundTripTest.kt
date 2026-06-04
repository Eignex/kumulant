package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.regression.glm.CovarianceRegressionResult
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionResult
import com.eignex.kumulant.stat.regression.tree.RegressionTreeConfig
import com.eignex.kumulant.stat.regression.tree.ThresholdSplit
import com.eignex.kumulant.stat.regression.tree.TreeRegressionResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-9
private fun feat(vararg xs: Double) = DenseVector.of(xs)

class RegressionSpecsRoundTripTest {

    @Test fun `BayesianRegression leaf spec round trips and materializes`() {
        val cfg: RegressionStatSpec<CovarianceRegressionResult> = BayesianRegression(
            featureSize = 2,
            priorVariance = 0.5,
        )
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        val live = decoded.materialize(Concurrency.None)
        live.update(feat(1.0, 0.0), y = 1.0)
        live.update(feat(0.0, 1.0), y = 2.0)
        assertEquals(2, live.featureSize)
    }

    @Test fun `StochasticRegression leaf spec round trips`() {
        val cfg: RegressionStatSpec<StochasticRegressionResult> = StochasticRegression(featureSize = 1)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        decoded.materialize(Concurrency.None).update(feat(0.5), y = 1.0)
    }

    @Test fun `StochasticRegression with Adam optimizer round trips`() {
        val cfg = StochasticRegression(featureSize = 2, optimizer = Adam(beta1 = 0.95, beta2 = 0.999))
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        assertEquals(cfg, SchemaJson.decodeFromString<StatSpec>(json))
    }

    @Test fun `StochasticRegression with Adagrad and Rmsprop round trip`() {
        for (opt in listOf(Adagrad(epsilon = 1e-8), Rmsprop(rho = 0.95))) {
            val cfg = StochasticRegression(featureSize = 2, optimizer = opt)
            val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
            assertEquals(cfg, SchemaJson.decodeFromString<StatSpec>(json))
        }
    }

    @Test fun `DiagonalRegression leaf spec round trips`() {
        val cfg = DiagonalRegression(featureSize = 2, priorPrecision = 2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        decoded.materialize(Concurrency.None).update(feat(1.0, 1.0), y = 1.0)
    }

    @Test fun `SoftmaxRegression leaf spec round trips`() {
        val cfg = SoftmaxRegression(featureSize = 2, numClasses = 3)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        decoded.materialize(Concurrency.None).update(feat(1.0, 0.5), y = 1.0)
    }

    @Test fun `GaussianNaiveBayes leaf spec round trips`() {
        val cfg = GaussianNaiveBayes(featureSize = 2, numClasses = 3, varianceFloor = 1e-6)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        decoded.materialize(Concurrency.None).update(feat(0.5, 0.5), y = 1.0)
    }

    @Test fun `DecisionTreeRegression leaf spec round trips with RegressionTreeConfig`() {
        val cfg = DecisionTreeRegression(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            config = RegressionTreeConfig(splitPeriod = 8),
        )
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        val live = decoded.materialize(Concurrency.None)
        repeat(5) { live.update(feat(if (it % 2 == 0) -1.0 else 1.0), y = 1.0) }
        assertEquals(5.0, (live.read(0L) as TreeRegressionResult).totalWeights, DELTA)
    }

    @Test fun `RandomForestRegression leaf spec round trips`() {
        val cfg = RandomForestRegression(
            featureSize = 1,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            nbrTrees = 3,
            bagging = false,
        )
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        decoded.materialize(Concurrency.None).update(feat(0.5), y = 1.0)
    }

    @Test fun `DecisionTreeClassifier leaf spec round trips`() {
        val cfg = DecisionTreeClassifier(
            featureSize = 2,
            numClasses = 3,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
        )
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        decoded.materialize(Concurrency.None).update(feat(0.5, 0.5), y = 2.0)
    }

    @Test fun `RandomForestClassifier leaf spec round trips`() {
        val cfg = RandomForestClassifier(
            featureSize = 2,
            numClasses = 3,
            splitCandidates = listOf(ThresholdSplit(0, 0.0)),
            nbrTrees = 4,
        )
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        decoded.materialize(Concurrency.None).update(feat(0.5, 0.5), y = 1.0)
    }

    @Test fun `filter decorator spec drops failing predicate`() {
        val cfg = StochasticRegression(featureSize = 1).filter(Y gt 0.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        val live = decoded.materialize(Concurrency.None)
        live.update(feat(0.0), y = -1.0) // dropped
        live.update(feat(0.0), y = 1.0) // kept
    }

    @Test fun `weightBy decorator spec multiplies weight by AST expression`() {
        val cfg = DiagonalRegression(featureSize = 1).weightBy(Y * Y)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        decoded.materialize(Concurrency.None).update(feat(0.0), y = 3.0)
    }

    @Test fun `throttle and sample decorator specs round trip`() {
        val throttled = StochasticRegression(featureSize = 1).throttle(every = 5)
        val sampled = StochasticRegression(featureSize = 1).sample(rate = 0.3, seed = 11L)
        for (cfg in listOf(throttled, sampled)) {
            val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
            val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
            decoded.materialize(Concurrency.None).update(feat(0.0), y = 1.0)
        }
    }

    @Test fun `foldRegression lifts a series spec into the regression modality`() {
        // Project Y through the series spec; the inner Sum sees y per update.
        val cfg: RegressionStatSpec<SumResult> = Sum.foldRegression(featureSize = 1, project = Y)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        val live = decoded.materialize(Concurrency.None)
        for (y in doubleArrayOf(1.0, 2.0, 3.0)) live.update(feat(0.5), y)
        assertEquals(6.0, (live.read(0L) as SumResult).sum, DELTA)
    }

    @Test fun `transformY decorator spec rewrites y via AST expression`() {
        val cfg: RegressionStatSpec<SumResult> = Sum.foldRegression(featureSize = 1, project = Y)
            .transformY(Y * 2.0)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as RegressionStatSpec<*>
        val live = decoded.materialize(Concurrency.None)
        live.update(feat(0.0), y = 3.0) // y rewritten to 6.0 before fold projects Y to inner
        assertEquals(6.0, (live.read(0L) as SumResult).sum, DELTA)
    }
}
