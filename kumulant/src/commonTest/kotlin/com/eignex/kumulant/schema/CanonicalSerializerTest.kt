@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package com.eignex.kumulant.schema

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.kumulant.bandit.contextual.ContextualBanditSpec
import com.eignex.kumulant.bandit.contextual.RegressionContextualSpec
import com.eignex.kumulant.bandit.contextual.TrackedContextualSpec
import com.eignex.kumulant.bandit.univariate.*
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.regression.glm.LinUcb
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.skema.SchemaJson
import kotlinx.serialization.SerializationException
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import kotlinx.serialization.protobuf.ProtoBuf
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CanonicalSerializerTest {
    @Test
    fun `ewma uses the canonical shape through standard JSON entry points`() {
        val spec: StatSpec = EwmaMean(Alpha(0.1))

        assertEquals("""{"ewma":{"alpha":0.1}}""", Json.encodeToString(spec))
        assertEquals(spec, Json.decodeFromString<StatSpec>("""{"ewma":{"alpha":0.1}}"""))
    }

    @Test
    fun `transformed ewma uses structured arithmetic through schema JSON`() {
        val spec: StatSpec = EwmaMean(Alpha(0.1)).transform(X + 1.0)
        val expected = """{"transform":{"expr":{"add":["x",1.0]},"inner":{"ewma":{"alpha":0.1}}}}"""

        val encoded = SchemaJson.encodeToString(spec)
        val decoded = SchemaJson.decodeFromString<StatSpec>(expected)

        assertEquals(Json.parseToJsonElement(expected), Json.parseToJsonElement(encoded))
        assertEquals(spec, decoded)
    }

    @Test
    fun `scalar nodes have stable documented shapes`() {
        val cases = listOf(
            X to """"x"""",
            Y to """"y"""",
            VIndex to """"index"""",
            Center to """"center"""",
            Scale to """"scale"""",
            Low to """"low"""",
            High to """"high"""",
            V(2) to """{"element":2}""",
            Const(2.0) to "2.0",
            Add(X, Const(2.0)) to """{"add":["x",2.0]}""",
            Sub(X, Const(2.0)) to """{"sub":["x",2.0]}""",
            Mul(X, Const(2.0)) to """{"mul":["x",2.0]}""",
            Div(X, Const(2.0)) to """{"div":["x",2.0]}""",
            Neg(X) to """{"neg":"x"}""",
            Abs(X) to """{"abs":"x"}""",
            Log(X) to """{"log":"x"}""",
            Exp(X) to """{"exp":"x"}""",
            Sqrt(X) to """{"sqrt":"x"}""",
            Pow(X, Const(2.0)) to """{"pow":["x",2.0]}""",
            MinExpr(X, Const(2.0)) to """{"min":["x",2.0]}""",
            MaxExpr(X, Const(2.0)) to """{"max":["x",2.0]}""",
            IfExpr(Gt(X, Const(0.0)), X, Const(0.0)) to
                """{"if":{"cond":{"gt":["x",0.0]},"then":"x","otherwise":0.0}}""",
            Switch(X, listOf(SwitchCase(1.0, Y)), Const(0.0)) to
                (
                    """{"switch":{"on":"x","cases":[{"value":1.0,"then":"y"}],""" +
                        """"otherwise":0.0}}"""
                    ),
            Standardize to """"standardize"""",
            MinMax() to """"minMax"""",
            VFold(VFoldOp.Sum) to """{"fold":{"op":"Sum"}}""",
            VDot(listOf(1.0, 2.0)) to """{"dot":[1.0,2.0]}""",
        )
        for ((expr, expected) in cases) {
            assertEquals(expected, Json.encodeToString<ScalarExpr>(expr))
            assertEquals(expr, Json.decodeFromString<ScalarExpr>(expected))
        }
    }

    @Test
    fun `boolean nodes have stable documented shapes`() {
        val cases = listOf(
            Gt(X, Const(0.0)) to """{"gt":["x",0.0]}""",
            Ge(X, Const(0.0)) to """{"ge":["x",0.0]}""",
            Lt(X, Const(0.0)) to """{"lt":["x",0.0]}""",
            Le(X, Const(0.0)) to """{"le":["x",0.0]}""",
            Eq(X, Const(0.0)) to """{"eq":["x",0.0]}""",
            And(AllFinite, AllFinite) to """{"and":["allFinite","allFinite"]}""",
            Or(AllFinite, AllFinite) to """{"or":["allFinite","allFinite"]}""",
            Not(AllFinite) to """{"not":"allFinite"}""",
            In(X, listOf(1.0)) to """{"in":{"of":"x","values":[1.0]}}""",
            IsNaN(X) to """{"isNaN":"x"}""",
            IsFinite(X) to """{"isFinite":"x"}""",
            AllFinite to """"allFinite"""",
            InRange(X, 0.0, 1.0) to """{"inRange":{"a":"x","min":0.0,"max":1.0}}""",
        )
        for ((expr, expected) in cases) {
            assertEquals(expected, Json.encodeToString<BoolExpr>(expr))
            assertEquals(expr, Json.decodeFromString<BoolExpr>(expected))
        }
    }

    @Test
    fun `vector expressions keep nested scalar expressions structured`() {
        val expr = vectorOf(V(1), X + Center)
        val expected = """{"elements":[{"element":1},{"add":["x","center"]}]}"""

        assertEquals(expected, Json.encodeToString<VectorExpr>(expr))
        assertEquals(expr, Json.decodeFromString<VectorExpr>(expected))
    }

    @Test
    fun `decorator order survives encoding and materialization`() {
        val before = Mean.transform(X + 1.0).filter(X gt 0.0)
        val after = Mean.filter(X gt 0.0).transform(X + 1.0)
        val specs = listOf(before, after)
        val observed = specs.map { spec ->
            val encoded = Json.encodeToString<StatSpec>(spec)
            val decoded = Json.decodeFromString<StatSpec>(encoded) as SeriesStatSpec<*>
            assertEquals(spec, decoded)
            val stat = decoded.materialize()
            stat.update(-0.5)
            stat.update(1.0)
            (stat.read() as WeightedMeanResult).mean
        }

        assertEquals(listOf(2.0, 1.25), observed)
    }

    @Test
    fun `typed modality roots use the same codec as StatSpec`() {
        val spec: SeriesStatSpec<WeightedMeanResult> = EwmaMean(Alpha(0.1))

        assertEquals("""{"ewma":{"alpha":0.1}}""", Json.encodeToString(spec))
        assertEquals(spec, Json.decodeFromString<SeriesStatSpec<WeightedMeanResult>>(Json.encodeToString(spec)))
    }

    @Test
    fun `decay and optimizer configurations use stable operator keys`() {
        val decay = listOf<DecayWeightingSpec>(Alpha(0.2), HalfLife(1000))
        val optimizers = listOf<OptimizerSpec>(Sgd(X + 1.0), Adagrad(), Rmsprop(), Adam())
        for (spec in decay) {
            val encoded = Json.encodeToString(spec)
            assertEquals(spec, Json.decodeFromString<DecayWeightingSpec>(encoded))
            assertFalse("type" in encoded)
        }
        for (spec in optimizers) {
            val encoded = Json.encodeToString(spec)
            assertEquals(spec, Json.decodeFromString<OptimizerSpec>(encoded))
            assertFalse("type" in encoded)
        }
        assertEquals(
            """{"decayingMean":{"halfLifeMillis":1000}}""",
            Json.encodeToString<StatSpec>(DecayingMean(HalfLife(1000))),
        )
        assertEquals("""{"holt":{"alpha":0.2}}""", Json.encodeToString<StatSpec>(Holt(Alpha(0.2))))
    }

    @Test
    fun `bandits carry canonical composed regression specs and expressions`() {
        val contextual: ContextualBanditSpec = RegressionContextualSpec(
            nbrArms = 2,
            regression = BayesianRegression(2).transformY(X + 1.0),
            posterior = LinUcb,
            globalRegression = BayesianRegression(2).transformY(Y + 1.0),
        )
        val composite = CompositeArm(listOf(CompositeSubArm(BernoulliArm(), valueExpr = X + 1.0, filter = X gt 0.0)))
        val univariate: UnivariateBanditSpec = MultiArmedSpec(
            2,
            ThompsonSamplingSpec(composite, CompositePosterior(listOf(BetaPosterior), V(0))),
        )
        val encoded = Json.encodeToString(contextual)
        val rewards = Json.encodeToString(univariate)

        assertTrue("\"linUcb\"" in encoded)
        assertTrue(""""add":["x",1.0]""" in encoded)
        assertEquals(contextual, Json.decodeFromString<ContextualBanditSpec>(encoded))
        assertEquals(univariate, Json.decodeFromString<UnivariateBanditSpec>(rewards))
        assertFalse("type" in rewards)
    }

    @Test
    fun `JSON settings control default fields without changing operator shapes`() {
        val defaults = Json { encodeDefaults = true }
        val spec: StatSpec = Holt(Alpha(0.2))

        assertEquals("""{"holt":{"alpha":0.2}}""", Json.encodeToString(spec))
        assertEquals("""{"holt":{"alpha":0.2,"beta":0.2,"phi":1.0}}""", defaults.encodeToString(spec))
        assertEquals(spec, Json.decodeFromString<StatSpec>(defaults.encodeToString(spec)))
    }

    @Test
    fun `default encoding expands optional fields on named nodes`() {
        val defaults = Json { encodeDefaults = true }
        val expr: ScalarExpr = MinMax()

        assertEquals("\"minMax\"", Json.encodeToString(expr))
        assertEquals("""{"minMax":{"targetLow":0.0,"targetHigh":1.0}}""", defaults.encodeToString(expr))
        assertEquals(expr, Json.decodeFromString<ScalarExpr>(defaults.encodeToString(expr)))
    }

    @Test
    fun `finite filters retain named fields around short stat names`() {
        val spec: StatSpec = Mean.filterFinite()
        val expected = """{"filter":{"inner":"mean","pred":"allFinite"}}"""

        assertEquals(Json.parseToJsonElement(expected), Json.parseToJsonElement(Json.encodeToString(spec)))
        assertEquals(spec, Json.decodeFromString<StatSpec>(expected))
    }

    @Test
    fun `invalid expression shapes report the offending path`() {
        val cases = listOf(
            """{"missing":[]}""" to "$.missing",
            """{"add":[1]}""" to "$.add",
            """{"add":{"l":1,"r":2}}""" to "$.add",
            """{"input":"unknown"}""" to "$.input",
            """"unknown"""" to "$.unknown",
            """{"const":1}""" to "$.const",
            """{"add":[1,{"missing":{}}]}""" to "$.add[1].missing",
            """{"neg":true}""" to "$.neg",
            """{"element":{}}""" to "$.element",
            """{"standardize":{"unexpected":0}}""" to "$.standardize.unexpected",
            """{"add":[1,2],"neg":3}""" to "$",
            "\"1 + value\"" to "$",
        )
        for ((payload, path) in cases) {
            val error = assertFailsWith<SerializationException>(payload) { Json.decodeFromString<ScalarExpr>(payload) }
            assertTrue(path in error.message.orEmpty(), error.message)
        }
    }

    @Test
    fun `invalid spec shapes reject old JSON and unknown fields`() {
        val lenient = Json {
            ignoreUnknownKeys = true
            coerceInputValues = true
        }
        val cases = listOf(
            """{"type":"Mean"}""",
            """{"ewma":{"weighting":{"alpha":0.1}}}""",
            """{"ewma":{}}""",
            """{"ewma":{"alpha":0.1,"extra":1}}""",
            """{"mean":{"extra":1}}""",
            """"ewma"""",
            """{"filter":["allFinite"]}""",
            """{"filter":["allFinite","mean", "sum"]}""",
            """{"filter":{"pred":"allFinite","inner":1}}""",
            """{"filter":{"pred":"allFinite","inner":{"transform":{"expr":"x","inner":1}}}}""",
            """{"transform":{"expr":"x","inner":"missing"}}""",
            """{"transform":{"expr":"x","inner":{"bayesianRegression":{"featureSize":2}}}}""",
            """{"ewma":{"alpha":"0.1"}}""",
            """{"holt":{"alphaWeighting":{"alpha":0.2}}}""",
            """{"stochasticRegression":{"featureSize":1,"optimizer":{"adam":{"learningRate":null}}}}""",
            """{"transform":{"expr":{"missing":[]},"inner":"mean"}}""",
        )
        for (payload in cases) {
            val error = assertFailsWith<SerializationException>(payload) { lenient.decodeFromString<StatSpec>(payload) }
            assertTrue("$" in error.message.orEmpty(), error.message)
        }
    }

    @Test
    fun `formats round trip composed specs across modalities`() {
        val specs: List<StatSpec> = listOf(
            EwmaMean(Alpha(0.1)).transform(X + 1.0).filter(X gt 0.0),
            UnivariateRegression().filter(X gt 0.0),
            Mean.vectorized(2).transformVector(vectorOf(V(1), V(0))),
            HyperLogLog().filter(X.isFinite()),
            StochasticRegression(2, Adam(Const(0.1))).transformY(Y + 1.0),
            PairedGroupStatSpec(mapOf("metric" to Sum.atX())).filterFinite(),
            VectorGroupStatSpec(mapOf("metric" to Sum.atIndex(0))).filterFinite(),
            DiscreteGroupStatSpec(mapOf("metric" to Sum.asDiscrete())).filter(X gt 0.0),
            RegressionGroupStatSpec(mapOf("metric" to Sum.foldRegression(1, Y))).filterFinite(),
            PairedSum.withFeedback(Variance, Variance, Center + VIndex),
            Sum.vectorized(2).withFeedback(Variance.vectorized(2), Center + VIndex),
            BayesianRegression(2).withFeedback(Variance.vectorized(2), Center),
            BayesianRegression(
                2,
                priorMean = DenseVector.of(doubleArrayOf(1.0, 2.0)),
                priorCovariance = DenseMatrix.ofRows(arrayOf(doubleArrayOf(2.0, 0.5), doubleArrayOf(0.5, 1.0))),
            ),
            DecisionTreeRegression(1, emptyList(), leafArm = Variance.withWeight(2.0)),
            RandomForestRegression(1, emptyList(), leafArm = Variance.withWeight(2.0)),
            DecisionTreeClassifier(1, 2, emptyList(), leafArm = ClassCounts(2).withWeight(2.0)),
            RandomForestClassifier(1, 2, emptyList(), leafArm = ClassCounts(2).withWeight(2.0)),
        )

        for (spec in specs) {
            assertEquals(spec, Json.decodeFromString<StatSpec>(Json.encodeToString(spec)))
        }
        for (format in listOf(ProtoBuf, Cbor)) {
            for (spec in specs) {
                val encoded = format.encodeToByteArray(StatSpec.serializer(), spec)
                assertEquals(spec, format.decodeFromByteArray(StatSpec.serializer(), encoded))
            }
        }
    }

    @Test
    fun `binary formats round trip expression roots`() {
        val scalar: ScalarExpr = IfExpr(X gt 0.0, X + Center, Scale)
        val predicate: BoolExpr = (X gt 0.0) and V(0).isFinite()
        val vector: VectorExpr = vectorOf(V(1), X + Center)

        for (format in listOf(ProtoBuf, Cbor)) {
            assertEquals(scalar, format.decodeFromByteArray<ScalarExpr>(format.encodeToByteArray(scalar)))
            assertEquals(predicate, format.decodeFromByteArray<BoolExpr>(format.encodeToByteArray(predicate)))
            assertEquals(vector, format.decodeFromByteArray<VectorExpr>(format.encodeToByteArray(vector)))
        }
    }

    @Test
    fun `formats round trip bandit configurations`() {
        val contextual: ContextualBanditSpec = RegressionContextualSpec(
            nbrArms = 2,
            regression = BayesianRegression(2).transformY(Y + 1.0),
            posterior = LinUcb,
            globalRegression = BayesianRegression(2).transformY(Y + 1.0),
        )
        val univariate: UnivariateBanditSpec = MultiArmedSpec(2, Ucb1Spec())
        val contextualSpecs = listOf(
            contextual,
            TrackedContextualSpec(contextual, 2, updateMarginalTemplate = BayesianRegression(2)),
        )
        val univariateSpecs = listOf(
            univariate,
            TrackedUnivariateSpec(univariate, chooseTemplate = Count, updateArmRewardTemplate = PairedSum),
        )
        for (spec in contextualSpecs) {
            assertEquals(spec, Json.decodeFromString<ContextualBanditSpec>(Json.encodeToString(spec)))
        }
        for (spec in univariateSpecs) {
            assertEquals(spec, Json.decodeFromString<UnivariateBanditSpec>(Json.encodeToString(spec)))
        }

        for (format in listOf(ProtoBuf, Cbor)) {
            for (spec in contextualSpecs) {
                assertEquals(spec, format.decodeFromByteArray<ContextualBanditSpec>(format.encodeToByteArray(spec)))
            }
            for (spec in univariateSpecs) {
                assertEquals(spec, format.decodeFromByteArray<UnivariateBanditSpec>(format.encodeToByteArray(spec)))
            }
        }
    }

    @Test
    fun `binary serialization retains generated class tags and operand fields`() {
        val expr: ScalarExpr = X + 1.0
        val encoded = ProtoBuf.encodeToByteArray(expr)
        val text = encoded.decodeToString()

        assertEquals("0a03416464121b0a050a0158120012120a05436f6e7374120909000000000000f03f", encoded.toHexString())
        assertTrue("Add" in text)
        assertTrue("Const" in text)
        assertEquals(expr, ProtoBuf.decodeFromByteArray<ScalarExpr>(encoded))
        val spec: StatSpec = EwmaMean(Alpha(0.1)).transform(expr)
        assertEquals(spec, ProtoBuf.decodeFromByteArray<StatSpec>(ProtoBuf.encodeToByteArray(spec)))
    }
}
