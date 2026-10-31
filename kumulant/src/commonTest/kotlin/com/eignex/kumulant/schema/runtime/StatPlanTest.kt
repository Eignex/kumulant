package com.eignex.kumulant.schema.runtime

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.feat
import com.eignex.kumulant.schema.ops.band
import com.eignex.kumulant.schema.ops.withWeight
import com.eignex.kumulant.schema.spec.ClassCounts
import com.eignex.kumulant.schema.spec.DDSketch
import com.eignex.kumulant.schema.spec.DecisionTreeClassifier
import com.eignex.kumulant.schema.spec.DecisionTreeRegression
import com.eignex.kumulant.schema.spec.GroupStatSpec
import com.eignex.kumulant.schema.spec.PairedSum
import com.eignex.kumulant.schema.spec.RandomForestClassifier
import com.eignex.kumulant.schema.spec.RandomForestRegression
import com.eignex.kumulant.schema.spec.RegressionStatSpec
import com.eignex.kumulant.schema.spec.StatSpec
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.schema.spec.Variance
import com.eignex.kumulant.schema.spec.WithWeightSeries
import com.eignex.kumulant.stat.regression.tree.DecisionTreeClassifierStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.regression.tree.RandomForestClassifierStat
import com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class StatPlanTest {
    @Test
    fun `nested modality failures include the full schema path`() {
        val schema = StatSchemaDef(
            mapOf("outer" to GroupStatSpec(mapOf("metric" to WithWeightSeries(PairedSum, 2.0)))),
        )

        val failure = assertFailsWith<IllegalArgumentException> { schema.materialize() }

        assertTrue(failure.message.orEmpty().contains("outer.metric.inner"))
    }

    @Test
    fun `every entry is structurally checked before constructing accumulators`() {
        val schema = StatSchemaDef(
            mapOf(
                "invalidParameters" to DDSketch(relativeError = -1.0),
                "invalidModality" to WithWeightSeries(PairedSum, 2.0),
            ),
        )

        val failure = assertFailsWith<IllegalArgumentException> { schema.materializeSeries() }

        assertTrue(failure.message.orEmpty().contains("invalidModality.inner"))
    }

    @Test
    fun `constructor failures identify the schema entry`() {
        val schema = StatSchemaDef(mapOf("latency" to DDSketch(relativeError = -1.0)))

        val failure = assertFailsWith<IllegalArgumentException> { schema.materializeSeries() }

        assertTrue(failure.message.orEmpty().contains("latency"))
        assertTrue(failure.message.orEmpty().contains("Relative error"))
    }

    @Test
    fun `a compiled plan constructs independent stats at the requested modes`() {
        val plan = Variance.withWeight(2.0).compileSeries("variance")

        val first = plan.build(Concurrency.None)
        val second = plan.build(Concurrency.Strict)
        first.update(3.0, 0L)

        assertEquals(Concurrency.None, first.concurrency)
        assertEquals(Concurrency.Strict, second.concurrency)
        assertEquals(2.0, assertIs<WeightedVarianceResult>(first.read(0L)).totalWeights)
        assertEquals(0.0, assertIs<WeightedVarianceResult>(second.read(0L)).totalWeights)
    }

    @Test
    fun `band requirements are checked through update adapters`() {
        val schema = StatSchemaDef(mapOf("band" to Sum.withWeight(2.0).band(1.0)))

        val failure = assertFailsWith<IllegalArgumentException> { schema.materializeSeries() }

        assertTrue(failure.message.orEmpty().contains("band.inner"))
    }

    @Test
    fun `band accepts a center scale result through update adapters`() {
        val stat = Variance.withWeight(2.0).band(1.0).materialize()

        stat.update(1.0, 0L)
        stat.update(3.0, 1L)

        assertEquals(1.0, stat.read(1L).lower)
        assertEquals(3.0, stat.read(1L).upper)
    }

    @Test
    fun `decoded leaf recipes reject incompatible results before updates`() {
        val cases = listOf(
            """{"decisionTreeRegression":{"featureSize":1,"splitCandidates":[],"leafArm":"sum"}}""",
            """{"randomForestRegression":{"featureSize":1,"splitCandidates":[],"leafArm":"sum"}}""",
            """{"decisionTreeClassifier":{"featureSize":1,"numClasses":2,"splitCandidates":[],"leafArm":"sum"}}""",
            """{"randomForestClassifier":{"featureSize":1,"numClasses":2,"splitCandidates":[],"leafArm":"sum"}}""",
            """{"decisionTreeClassifier":{"featureSize":1,"numClasses":2,"splitCandidates":[],""" +
                """"leafArm":{"classCounts":{"numClasses":3}}}}""",
        )
        for (json in cases) {
            val spec = SchemaJson.decodeFromString<StatSpec>(json)

            val error = assertFailsWith<IllegalArgumentException> { spec.materialize() }

            assertTrue("leafArm" in error.message.orEmpty(), error.message)
        }
    }

    @Test
    fun `replicas of spec built trees grow leaf arms at the replica concurrency`() {
        val x = feat(0.0)
        val cases: List<Pair<RegressionStatSpec<*>, (RegressionStat<*>) -> Concurrency>> = listOf(
            DecisionTreeRegression(1, emptyList(), leafArm = Variance) to { stat ->
                (stat as DecisionTreeRegressionStat).tree().findLeaf(x).arm.concurrency
            },
            RandomForestRegression(1, emptyList(), nbrTrees = 1, leafArm = Variance) to { stat ->
                (stat as RandomForestRegressionStat).findLeaves(x).single().arm.concurrency
            },
            DecisionTreeClassifier(1, 2, emptyList(), leafArm = ClassCounts(2)) to { stat ->
                (stat as DecisionTreeClassifierStat).tree().findLeaf(x).arm.concurrency
            },
            RandomForestClassifier(1, 2, emptyList(), nbrTrees = 1, leafArm = ClassCounts(2)) to { stat ->
                (stat as RandomForestClassifierStat).trees().single().findLeaf(x).arm.concurrency
            },
        )
        for ((spec, leafConcurrency) in cases) {
            val replica = spec.materialize(Concurrency.None).create(Concurrency.Strict)

            assertEquals(Concurrency.Strict, leafConcurrency(replica), spec.toString())
        }
    }

    @Test
    fun `decoded feedback requires per coordinate primary results`() {
        val json = """
            {"feedback": {
                "inner": {"vectorize": {"dimensions": 2, "template": "sum"}},
                "primary": {"foldVector": {"inner": "sum", "expr": {"fold": {"op": "Sum"}}}},
                "project": {"input": "x"}
            }}
        """.trimIndent()
        val spec = SchemaJson.decodeFromString<StatSpec>(json)

        val error = assertFailsWith<IllegalArgumentException> { spec.materialize() }

        assertTrue("primary" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `regression feedback checks the primary feature count before updates`() {
        val json = """
            {"feedback": {
                "inner": {"bayesianRegression": {"featureSize": 2}},
                "primary": {"vectorize": {"dimensions": 1, "template": "variance"}},
                "project": {"input": "x"}
            }}
        """.trimIndent()
        val spec = SchemaJson.decodeFromString<StatSpec>(json)

        val error = assertFailsWith<IllegalArgumentException> { spec.materialize() }

        assertTrue("primary" in error.message.orEmpty(), error.message)
    }
}
