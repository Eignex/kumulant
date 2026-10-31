package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.materialize
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.feat
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.regression.RegressionPosteriorSerializer
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.tree.*
import com.eignex.skema.SchemaJson
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.protobuf.ProtoBuf
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ContextualBanditSpecTest {

    @Test
    fun `expressions control contextual updates and choices`() {
        val regression = BayesianRegression(featureSize = 1).transformY(Y * 2.0)
            .weightBy(V(0) * V(0)).filter(Y gt 0.0)
        val spec: ContextualBanditSpec = RegressionContextualSpec(2, regression, LinUcb, exploration = 0.0)
        val bandit = spec.materialize(Random(0)) as ContextualBandit
        val direct = regression.materialize()
        val x = feat(2.0)

        bandit.update(0, x, -100.0)
        bandit.update(0, x, 3.0, weight = 0.5)
        direct.update(x, 3.0, weight = 0.5)

        val result = (bandit as RegressionContextualBandit<*>).armResult(0) as PrecisionRegressionResult
        assertEquals(direct.read(0L).predict(x), result.predict(x), DELTA)
        assertEquals(0, bandit.choose(x))
        assertEquals(0.0, bandit.evaluate(1, x), DELTA)
    }

    @Test
    fun `native linear tree and forest specs learn`() {
        val specs = listOf(
            RegressionContextualSpec(2, BayesianRegression(1), MultivariateGaussian, exploration = 0.0),
            RegressionContextualSpec(2, DiagonalRegression(1), FactorisedGaussian, exploration = 0.0),
            RegressionContextualSpec(2, StochasticRegression(1), PointPosterior, exploration = 0.0),
            RegressionContextualSpec(2, DecisionTreeRegression(1, emptyList()), ThompsonTreePosterior(2.0, 3.0)),
            RegressionContextualSpec(
                2,
                RandomForestRegression(1, emptyList(), 2, bagging = false),
                ThompsonTreePosterior(2.0, 3.0),
            ),
        )
        for (spec in specs) {
            val bandit = spec.copy(exploration = 0.0).materialize(Random(0))
            bandit.update(0, feat(1.0), 2.0)
            assertTrue(bandit.evaluate(0, feat(1.0)) > bandit.evaluate(1, feat(1.0)))
        }
    }

    @Test
    fun `global regression expressions materialize independently from arm expressions`() {
        val regression = BayesianRegression(1).withWeight(2.0)
        val global = BayesianRegression(1).transformY(Y * 3.0)
        val spec: ContextualBanditSpec = RegressionContextualSpec(2, regression, LinUcb, 0.0, global)
        val bandit = spec.materialize(Random(0), Concurrency.Strict) as RegressionContextualBandit<*>
        val directGlobal = global.materialize()
        val x = feat(1.0)

        bandit.update(0, x, 2.0)
        directGlobal.update(x, 2.0)

        val result = bandit.globalSnapshot() as PrecisionRegressionResult
        assertEquals(directGlobal.read(0L).predict(x), result.predict(x), DELTA)
        assertEquals(0.0, (bandit.armResult(1) as PrecisionRegressionResult).predict(x), DELTA)
    }

    @Test
    fun `decoded mismatched posterior is rejected during materialization`() {
        val encoded = """
            {"regressionContextual": {
                "nbrArms": 2,
                "regression": {"bayesianRegression": {"featureSize": 1}},
                "posterior": "pointPosterior"
            }}
        """.trimIndent()
        val decoded = SchemaJson.decodeFromString(ContextualBanditSpec.serializer(), encoded)

        val error = assertFailsWith<IllegalArgumentException> { decoded.materialize() }

        assertTrue(error.message.orEmpty().contains("regression"))
        assertTrue(error.message.orEmpty().contains("PointPosterior"))
    }

    @Test
    fun `global regression with incompatible feature dimension is rejected`() {
        val spec = RegressionContextualSpec(2, BayesianRegression(1), LinUcb, globalRegression = BayesianRegression(2))

        val error = assertFailsWith<IllegalArgumentException> { spec.materialize() }

        assertTrue(error.message.orEmpty().contains("globalRegression.featureSize"))
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Test
    fun `regression posteriors round trip through JSON and binary formats`() {
        val posteriors: List<RegressionPosterior<*>> = listOf(
            PointPosterior,
            FactorisedGaussian,
            MultivariateGaussian,
            LinUcb,
            MeanTreePosterior,
            ThompsonTreePosterior(2.0, 3.0),
            UcbTreePosterior(2.0, 3.0),
        )
        val json = Json { classDiscriminator = "kind" }
        for (posterior in posteriors) {
            val encodedJson = json.encodeToString(RegressionPosteriorSerializer, posterior)
            assertEquals(posterior, json.decodeFromString(RegressionPosteriorSerializer, encodedJson))
            val encodedBinary = ProtoBuf.encodeToByteArray(RegressionPosteriorSerializer, posterior)
            assertEquals(posterior, ProtoBuf.decodeFromByteArray(RegressionPosteriorSerializer, encodedBinary))
        }
    }

    @Test
    fun `decoded mismatched global regression is rejected during materialization`() {
        val encoded = """
            {"regressionContextual": {
                "nbrArms": 2,
                "regression": {"bayesianRegression": {"featureSize": 1}},
                "posterior": "linUcb",
                "globalRegression": {"stochasticRegression": {"featureSize": 1}}
            }}
        """.trimIndent()
        val decoded = Json.decodeFromString(ContextualBanditSpec.serializer(), encoded)

        val error = assertFailsWith<IllegalArgumentException> { decoded.materialize() }

        assertTrue(error.message.orEmpty().contains("globalRegression"))
        assertTrue(error.message.orEmpty().contains("StochasticRegressionResult"))
    }
}
