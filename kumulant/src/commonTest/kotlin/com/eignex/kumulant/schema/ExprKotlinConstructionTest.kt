package com.eignex.kumulant.schema

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.expr.BoolExpr
import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.V
import com.eignex.kumulant.schema.expr.VFoldOp
import com.eignex.kumulant.schema.expr.VectorExpr
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.abs
import com.eignex.kumulant.schema.expr.div
import com.eignex.kumulant.schema.expr.exp
import com.eignex.kumulant.schema.expr.inRange
import com.eignex.kumulant.schema.expr.ln
import com.eignex.kumulant.schema.expr.max
import com.eignex.kumulant.schema.expr.min
import com.eignex.kumulant.schema.expr.plus
import com.eignex.kumulant.schema.expr.pow
import com.eignex.kumulant.schema.expr.sqrt
import com.eignex.kumulant.schema.expr.vDot
import com.eignex.kumulant.schema.expr.vFold
import com.eignex.kumulant.schema.expr.vectorOf
import com.eignex.kumulant.schema.ops.transformVector
import com.eignex.kumulant.schema.ops.transformX
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.StochasticRegression
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.schema.spec.Vectorized
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Every AST node is reachable from Kotlin, and the Kotlin form agrees with the wire form.
 *
 * The library's rule is that an expression must be both serializable and constructable in Kotlin. The
 * round trip is the half that makes the pairing meaningful: a factory that produced a node the wire
 * could not carry, or a serial name that decoded to something else, would satisfy neither.
 */
class ExprKotlinConstructionTest {

    private val v = doubleArrayOf(2.0, 3.0, 4.0)

    private fun scalarRoundTrip(expr: ScalarExpr): ScalarExpr =
        SchemaJson.decodeFromString(ScalarExpr.serializer(), SchemaJson.encodeToString(ScalarExpr.serializer(), expr))

    private fun boolRoundTrip(expr: BoolExpr): BoolExpr =
        SchemaJson.decodeFromString(BoolExpr.serializer(), SchemaJson.encodeToString(BoolExpr.serializer(), expr))

    private fun vectorRoundTrip(expr: VectorExpr): VectorExpr =
        SchemaJson.decodeFromString(VectorExpr.serializer(), SchemaJson.encodeToString(VectorExpr.serializer(), expr))

    @Test
    fun `every scalar maths node builds in Kotlin and survives the wire`() {
        val cases: List<Triple<String, ScalarExpr, Double>> = listOf(
            Triple("abs", X.abs(), 2.5),
            Triple("ln", X.ln(), kotlin.math.ln(2.5)),
            Triple("exp", X.exp(), kotlin.math.exp(2.5)),
            Triple("sqrt", X.sqrt(), kotlin.math.sqrt(2.5)),
            Triple("pow(expr)", X.pow(Const(2.0)), 6.25),
            Triple("pow(const)", X.pow(2.0), 6.25),
            Triple("min(expr)", X.min(Const(1.0)), 1.0),
            Triple("min(const)", X.min(1.0), 1.0),
            Triple("max(expr)", X.max(Const(1.0)), 2.5),
            Triple("max(const)", X.max(1.0), 2.5),
        )
        val violations = mutableListOf<String>()
        for ((name, expr, expected) in cases) {
            val direct = expr.eval(2.5)
            if (kotlin.math.abs(direct - expected) > DELTA) {
                violations += "$name evaluated to $direct, expected $expected"
            }
            val revived = scalarRoundTrip(expr)
            if (revived != expr) violations += "$name did not survive the round trip: $revived"
            val afterWire = revived.eval(2.5)
            if (afterWire != direct) violations += "$name evaluated differently after the wire: $afterWire"
        }
        assertEquals(emptyList(), violations.toList(), "a scalar node is not usable from Kotlin")
    }

    @Test
    fun `a negative input still reaches the nodes that answer NaN`() {
        // The factories must not quietly guard their inputs: `ln` of a negative number and `sqrt` of one
        // are NaN, which is the documented behaviour and what the wire form does too.
        assertTrue(X.ln().eval(-1.0).isNaN(), "ln of a negative should be NaN")
        assertTrue(X.sqrt().eval(-1.0).isNaN(), "sqrt of a negative should be NaN")
        assertTrue(scalarRoundTrip(X.ln()).eval(-1.0).isNaN(), "and the same after a round trip")
    }

    @Test
    fun `inRange builds in Kotlin and is inclusive at both ends`() {
        val expr = X.inRange(1.0, 3.0)

        assertTrue(expr.eval(1.0), "the lower bound should be included")
        assertTrue(expr.eval(3.0), "the upper bound should be included")
        assertTrue(!expr.eval(0.999), "below the range should be excluded")
        assertTrue(!expr.eval(3.001), "above the range should be excluded")
        assertEquals(expr, boolRoundTrip(expr), "inRange did not survive the round trip")
    }

    @Test
    fun `every vector fold builds in Kotlin and survives the wire`() {
        val expected = mapOf(
            VFoldOp.Sum to 9.0,
            VFoldOp.Product to 24.0,
            VFoldOp.Mean to 3.0,
            VFoldOp.Min to 2.0,
            VFoldOp.Max to 4.0,
            VFoldOp.Norm2 to kotlin.math.sqrt(29.0),
        )
        // Driven off VFoldOp.entries, not a hand-written list, so a new op added later fails here
        // rather than going untested. That is how Norm2 was caught when this test was first written.
        val violations = mutableListOf<String>()
        for (op in VFoldOp.entries) {
            val expr = vFold(op)
            val want = expected[op]
            if (want == null) {
                violations += "$op has no expectation here, so this sweep does not cover it"
                continue
            }
            val got = expr.eval(0.0, 0.0, v)
            if (kotlin.math.abs(got - want) > DELTA) violations += "$op gave $got, expected $want"
            if (scalarRoundTrip(expr) != expr) violations += "$op did not survive the round trip"
        }
        assertEquals(emptyList(), violations.toList(), "a VFoldOp is not usable from Kotlin")
    }

    @Test
    fun `vDot builds in Kotlin in both forms and survives the wire`() {
        val fromList = vDot(listOf(1.0, 2.0, 3.0))
        val fromVarargs = vDot(1.0, 2.0, 3.0)

        assertEquals(fromList, fromVarargs, "the two forms should build the same node")
        assertEquals(20.0, fromList.eval(0.0, 0.0, v), DELTA)
        assertEquals(fromList, scalarRoundTrip(fromList), "vDot did not survive the round trip")
    }

    @Test
    fun `vectorOf is a Kotlin constructor for VectorExpr`() {
        // VectorExpr had no public implementation at all, so `transformVector` and `transformX` - both
        // public - could not be called from Kotlin. This is the constructor that closes that.
        val permute: VectorExpr = vectorOf(V(2), V(0), V(1))

        assertEquals(listOf(4.0, 2.0, 3.0), permute.eval(0.0, 0.0, v).toList())
        assertEquals(permute, vectorRoundTrip(permute), "vectorOf did not survive the round trip")
    }

    @Test
    fun `vectorOf composes with the scalar DSL and can change dimensionality`() {
        val pooled = vectorOf((V(0) + V(1)) / 2.0)

        assertEquals(listOf(2.5), pooled.eval(0.0, 0.0, v).toList(), "pooling should reduce to one entry")
        assertEquals(pooled, vectorRoundTrip(pooled), "the composed form did not survive the round trip")
    }

    @Test
    fun `a nested expression built entirely in Kotlin round-trips whole`() {
        // The nodes have to compose, not merely exist: this is one tree mixing seven of the new
        // factories with the pre-existing operator sugar.
        val expr = X.abs().sqrt().min(vDot(1.0, 0.0, 0.0)).max(Const(0.5)).pow(2.0) + vFold(VFoldOp.Mean)

        val direct = expr.eval(-16.0, 0.0, v)
        assertEquals(expr, scalarRoundTrip(expr), "the nested tree did not survive the round trip")
        assertEquals(direct, scalarRoundTrip(expr).eval(-16.0, 0.0, v), "the revived tree evaluates differently")
        // sqrt(abs(-16)) = 4, min(4, 2) = 2, max(2, 0.5) = 2, 2^2 = 4, plus mean(2,3,4) = 3 -> 7
        assertEquals(7.0, direct, DELTA)
    }

    @Test
    fun `the vector transforms are now reachable from Kotlin end to end`() {
        // The point of the exercise. `transformVector` and `transformX` are public and take a
        // VectorExpr, which had no public implementation, so neither could be called from Kotlin at all.
        // Materialising and driving one proves the whole path works, not just that the type exists.
        val reordered = Vectorized(dimensions = 3, template = Sum).transformVector(vectorOf(V(2), V(0), V(1)))
        val stat = reordered.materialize()
        stat.update(DenseVector.of(doubleArrayOf(1.0, 2.0, 3.0)))

        // Coordinate 0 of the output is input coordinate 2.
        assertEquals(3.0, stat.read().results[0].let { (it as com.eignex.kumulant.stat.summary.SumResult).sum }, DELTA)

        val regression = StochasticRegression(featureSize = 3).transformX(vectorOf(V(0), V(1), V(2)))
        val model = regression.materialize()
        model.update(DenseVector.of(doubleArrayOf(1.0, 1.0, 1.0)), 1.0)
        assertEquals(1.0, model.read().totalWeights, DELTA, "the regression transform did not accept an update")
    }
}
