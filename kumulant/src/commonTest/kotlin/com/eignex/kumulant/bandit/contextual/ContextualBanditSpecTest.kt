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
    fun `composed contextual regressions round trip through the sealed root`() {
        val spec: ContextualBanditSpec = RegressionContextualSpec(
            nbrArms = 2,
            regression = BayesianRegression(featureSize = 1).transformY(Y * 2.0)
                .weightBy(V(0) * V(0)).filter(Y gt 0.0),
            posterior = LinUcb,
            exploration = 0.0,
        )
        val encoded = SchemaJson.encodeToString(ContextualBanditSpec.serializer(), spec)
        val decoded = SchemaJson.decodeFromString(ContextualBanditSpec.serializer(), encoded)

        assertEquals(spec, decoded)
    }

    @Test
    fun `decoded expressions control contextual updates and choices`() {
        val regression = BayesianRegression(featureSize = 1).transformY(Y * 2.0)
            .weightBy(V(0) * V(0)).filter(Y gt 0.0)
        val spec: ContextualBanditSpec = RegressionContextualSpec(2, regression, LinUcb, exploration = 0.0)
        val encoded = SchemaJson.encodeToString(ContextualBanditSpec.serializer(), spec)
        val decoded = SchemaJson.decodeFromString(ContextualBanditSpec.serializer(), encoded)
        val bandit = decoded.materialize(Random(0)) as ContextualBandit
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
    fun `native linear tree and forest specs round trip and learn`() {
        val specs = listOf(
            RegressionContextualSpec(2, BayesianRegression(1), MultivariateGaussian, exploration = 0.0),
            RegressionContextualSpec(2, BayesianRegression(1), LinUcb, exploration = 0.0),
            RegressionContextualSpec(2, DiagonalRegression(1), FactorisedGaussian, exploration = 0.0),
            RegressionContextualSpec(2, StochasticRegression(1), PointPosterior, exploration = 0.0),
            RegressionContextualSpec(2, DecisionTreeRegression(1, emptyList()), MeanTreePosterior),
            RegressionContextualSpec(2, DecisionTreeRegression(1, emptyList()), ThompsonTreePosterior(2.0, 3.0)),
            RegressionContextualSpec(2, DecisionTreeRegression(1, emptyList()), UcbTreePosterior(2.0, 3.0)),
            RegressionContextualSpec(
                2,
                RandomForestRegression(1, emptyList(), 2, bagging = false),
                MeanForestPosterior,
            ),
            RegressionContextualSpec(
                2,
                RandomForestRegression(1, emptyList(), 2, bagging = false),
                ThompsonForestPosterior(2.0, 3.0),
            ),
            RegressionContextualSpec(
                2,
                RandomForestRegression(1, emptyList(), 2, bagging = false),
                UcbForestPosterior(2.0, 3.0),
            ),
        )
        for (format in listOf(SchemaJson, Json, Json { classDiscriminator = "kind" })) {
            for (spec in specs) {
                val encoded = format.encodeToString(ContextualBanditSpec.serializer(), spec)
                val decoded = format.decodeFromString(ContextualBanditSpec.serializer(), encoded)
                assertEquals(spec, decoded)
                val bandit = (decoded as RegressionContextualSpec<*>).copy(exploration = 0.0).materialize(Random(0))
                bandit.update(0, feat(1.0), 2.0)
                assertTrue(bandit.evaluate(0, feat(1.0)) > bandit.evaluate(1, feat(1.0)))
            }
        }
    }

    @Test
    fun `global regression expressions materialize independently from arm expressions`() {
        val regression = BayesianRegression(1).withWeight(2.0)
        val global = BayesianRegression(1).transformY(Y * 3.0)
        val spec: ContextualBanditSpec = RegressionContextualSpec(2, regression, LinUcb, 0.0, global)
        val encoded = SchemaJson.encodeToString(ContextualBanditSpec.serializer(), spec)
        val decoded = SchemaJson.decodeFromString(ContextualBanditSpec.serializer(), encoded)
        val bandit = decoded.materialize(Random(0), Concurrency.Strict) as RegressionContextualBandit<*>
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
        val spec: ContextualBanditSpec = RegressionContextualSpec(2, BayesianRegression(1), LinUcb)
        val encoded = SchemaJson.encodeToString(ContextualBanditSpec.serializer(), spec)
            .replace("linUcb", "pointPosterior")
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
    fun `regression posteriors round trip through a binary format`() {
        val posterior = ThompsonTreePosterior(priorWeight = 2.0, priorVariance = 3.0)

        val encoded = ProtoBuf.encodeToByteArray(RegressionPosteriorSerializer, posterior)
        val decoded = ProtoBuf.decodeFromByteArray(RegressionPosteriorSerializer, encoded)

        assertEquals(posterior, decoded)
    }

    @Test
    fun `decoded mismatched global regression is rejected during materialization`() {
        val encoded = """
            {"regressionContextual": {
                "nbrArms": 2,
                "regression": {"bayesianRegression": {"featureSize": 1}},
                "posterior": {"linUcb": {}},
                "globalRegression": {"stochasticRegression": {"featureSize": 1}}
            }}
        """.trimIndent()
        val decoded = Json.decodeFromString(ContextualBanditSpec.serializer(), encoded)

        val error = assertFailsWith<IllegalArgumentException> { decoded.materialize() }

        assertTrue(error.message.orEmpty().contains("globalRegression"))
        assertTrue(error.message.orEmpty().contains("StochasticRegressionResult"))
    }
}
