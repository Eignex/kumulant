package com.eignex.kumulant.schema

import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64StridedVectorView
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ExprTest {

    private class NoMaterializeVector(private val values: DoubleArray) : F64VectorLike {
        override val size: Int get() = values.size
        override fun get(i: Int): Double = values[i]
        override fun toDoubleArray(): DoubleArray = error("expression materialised its vector input")
    }

    @Test fun `vector aware scalar and bool expressions borrow input`() {
        val vector = NoMaterializeVector(doubleArrayOf(2.0, Double.POSITIVE_INFINITY, 4.0))

        assertEquals(8.0, (V(0) * V(2)).eval(v = vector), DELTA)
        assertEquals(true, (V(0) gt 1.0).eval(v = vector))
        assertEquals(false, allFinite().eval(v = vector))
    }

    @Test fun `vector aware vector expression allocates only its output`() {
        val vector = NoMaterializeVector(doubleArrayOf(2.0, 3.0, 4.0))

        assertTrue(VElements(listOf(V(2), V(0) + V(1))).eval(v = vector).contentEquals(doubleArrayOf(4.0, 5.0)))
    }

    @Test fun `vector aware coordinate access keeps bounds behavior`() {
        assertFailsWith<IndexOutOfBoundsException> { V(2).eval(v = NoMaterializeVector(doubleArrayOf(1.0))) }
    }

    @Test fun `vector aware expressions agree across vector representations`() {
        val dense = F64DenseVector.of(doubleArrayOf(2.0, 0.0, -3.0))
        val sparse = F64SparseVector.of(3, intArrayOf(0, 2), doubleArrayOf(2.0, -3.0))
        val strided = F64StridedVectorView(doubleArrayOf(2.0, 9.0, 0.0, 9.0, -3.0, 9.0), 0, 3, 2)
        val custom = NoMaterializeVector(doubleArrayOf(2.0, 0.0, -3.0))
        val scalar = V(0) * V(2) + VFold(VFoldOp.Sum)
        val predicate = (V(0) gt 0.0) and (V(2) lt 0.0)
        val vector = VElements(listOf(V(2), V(0) + V(1)))

        for (input in listOf<F64VectorLike>(dense, sparse, strided, custom)) {
            assertEquals(-7.0, scalar.eval(v = input), DELTA)
            assertTrue(predicate.eval(v = input))
            assertTrue(vector.eval(v = input).contentEquals(doubleArrayOf(-3.0, 2.0)))
            assertTrue(allFinite().eval(v = input))
        }
    }

    @Test fun `all finite checks stored non finite values and sparse zeroes`() {
        assertTrue(allFinite().eval(v = F64SparseVector.of(3, intArrayOf(1), doubleArrayOf(0.0))))
        assertEquals(false, allFinite().eval(v = F64SparseVector.of(3, intArrayOf(1), doubleArrayOf(Double.NaN))))
        assertEquals(
            false,
            allFinite().eval(v = F64SparseVector.of(3, intArrayOf(1), doubleArrayOf(Double.POSITIVE_INFINITY))),
        )
        assertTrue(allFinite().eval(v = NoMaterializeVector(doubleArrayOf())))
    }

    @Test fun `x returns input`() {
        assertEquals(3.0, X.eval(3.0), DELTA)
    }

    @Test fun `const ignores input`() {
        assertEquals(7.0, Const(7.0).eval(99.0), DELTA)
    }

    @Test fun `add sub mul div`() {
        assertEquals(5.0, Add(X, Const(2.0)).eval(3.0), DELTA)
        assertEquals(1.0, Sub(X, Const(2.0)).eval(3.0), DELTA)
        assertEquals(6.0, Mul(X, Const(2.0)).eval(3.0), DELTA)
        assertEquals(1.5, Div(X, Const(2.0)).eval(3.0), DELTA)
    }

    @Test fun `neg abs`() {
        assertEquals(-3.0, Neg(X).eval(3.0), DELTA)
        assertEquals(3.0, Abs(X).eval(-3.0), DELTA)
    }

    @Test fun `log exp sqrt pow`() {
        assertEquals(0.0, Log(Const(1.0)).eval(99.0), DELTA)
        assertEquals(1.0, Exp(Const(0.0)).eval(99.0), DELTA)
        assertEquals(3.0, Sqrt(Const(9.0)).eval(99.0), DELTA)
        assertEquals(8.0, Pow(Const(2.0), Const(3.0)).eval(99.0), DELTA)
    }

    @Test fun `min max`() {
        assertEquals(2.0, MinExpr(X, Const(2.0)).eval(5.0), DELTA)
        assertEquals(5.0, MaxExpr(X, Const(2.0)).eval(5.0), DELTA)
    }

    @Test fun `if branches`() {
        val expr = IfExpr(Gt(X, Const(0.0)), Const(1.0), Const(-1.0))
        assertEquals(1.0, expr.eval(0.5), DELTA)
        assertEquals(-1.0, expr.eval(-0.5), DELTA)
    }

    @Test fun `composed 2x plus 1`() {
        val expr = Add(Mul(Const(2.0), X), Const(1.0))
        assertEquals(7.0, expr.eval(3.0), DELTA)
    }

    @Test fun `clip via min max`() {
        val clip = MinExpr(Const(10.0), MaxExpr(Const(0.0), X))
        assertEquals(0.0, clip.eval(-5.0), DELTA)
        assertEquals(5.0, clip.eval(5.0), DELTA)
        assertEquals(10.0, clip.eval(20.0), DELTA)
    }

    @Test fun `gt lt ge le eq`() {
        assertEquals(true, Gt(X, Const(0.0)).eval(1.0))
        assertEquals(false, Gt(X, Const(0.0)).eval(0.0))
        assertEquals(true, Ge(X, Const(0.0)).eval(0.0))
        assertEquals(true, Lt(X, Const(0.0)).eval(-1.0))
        assertEquals(true, Le(X, Const(0.0)).eval(0.0))
        assertEquals(true, Eq(X, Const(5.0)).eval(5.0))
    }

    @Test fun `and or not`() {
        val a = Gt(X, Const(0.0))
        val b = Lt(X, Const(10.0))
        assertEquals(true, And(a, b).eval(5.0))
        assertEquals(false, And(a, b).eval(15.0))
        assertEquals(true, Or(a, b).eval(15.0))
        assertEquals(false, Not(a).eval(5.0))
    }

    @Test fun `in range inclusive`() {
        val r = InRange(X, 0.0, 10.0)
        assertEquals(true, r.eval(0.0))
        assertEquals(true, r.eval(10.0))
        assertEquals(true, r.eval(5.0))
        assertEquals(false, r.eval(-0.001))
        assertEquals(false, r.eval(10.001))
    }

    @Test fun `scalar expr round trips`() {
        val expr: ScalarExpr = Add(Mul(Const(2.0), X), Const(1.0))
        val json = SchemaJson.encodeToString(ScalarExpr.serializer(), expr)
        val decoded = SchemaJson.decodeFromString(ScalarExpr.serializer(), json)
        assertEquals(expr, decoded)
        assertEquals(7.0, decoded.eval(3.0), DELTA)
    }

    @Test fun `bool expr round trips`() {
        val pred: BoolExpr = And(Gt(X, Const(0.0)), Lt(X, Const(1.0)))
        val json = SchemaJson.encodeToString(BoolExpr.serializer(), pred)
        val decoded = SchemaJson.decodeFromString(BoolExpr.serializer(), json)
        assertEquals(pred, decoded)
        assertEquals(true, decoded.eval(0.5))
        assertEquals(false, decoded.eval(2.0))
    }

    @Test fun `if with bool round trips`() {
        val expr: ScalarExpr = IfExpr(InRange(X, 0.0, 1.0), X, Const(0.0))
        val json = SchemaJson.encodeToString(ScalarExpr.serializer(), expr)
        val decoded = SchemaJson.decodeFromString(ScalarExpr.serializer(), json)
        assertEquals(expr, decoded)
        assertEquals(0.5, decoded.eval(0.5), DELTA)
        assertEquals(0.0, decoded.eval(2.0), DELTA)
    }

    @Test fun `transform series applies expr per update`() {
        val cfg: SeriesStatSpec<SumResult> =
            Sum.transform(Add(Mul(Const(2.0), X), Const(1.0)))
        val live = cfg.materialize(Concurrency.None)
        live.update(1.0)
        live.update(2.0)
        live.update(3.0)
        // (2*1+1) + (2*2+1) + (2*3+1) = 15
        assertEquals(15.0, live.read().sum, DELTA)
    }

    @Test fun `filter series drops non matching updates`() {
        val cfg: SeriesStatSpec<SumResult> = Sum.filter(Gt(X, Const(0.0)))
        val live = cfg.materialize(Concurrency.None)
        live.update(-1.0)
        live.update(2.0)
        live.update(-3.0)
        live.update(4.0)
        assertEquals(6.0, live.read().sum, DELTA)
    }

    @Test fun `transform chained with other ops`() {
        val cfg: SeriesStatSpec<SumResult> =
            Sum.transform(MinExpr(Const(10.0), MaxExpr(Const(0.0), X)))
                .withWeight(2.0)
        val live = cfg.materialize(Concurrency.None)
        live.update(-5.0)
        live.update(5.0)
        live.update(20.0)
        // clipped 0,5,10 with weight 2 each: sum = 30
        assertEquals(30.0, live.read().sum, DELTA)
    }

    @Test fun `transform config round trips`() {
        val cfg: SeriesStatSpec<SumResult> =
            Sum.transform(MinExpr(Const(10.0), MaxExpr(Const(0.0), X)))
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        val live = (decoded as SeriesStatSpec<*>).materialize(Concurrency.None)
        live.update(-1.0)
        live.update(5.0)
        live.update(20.0)
        val sum = (live.read() as SumResult).sum
        assertEquals(15.0, sum, DELTA)
    }

    @Test fun `filter discrete drops non matching long updates`() {
        val cfg: DiscreteStatSpec<*> = HyperLogLog(precision = 10).filter(Ge(X, Const(0.0)))
        val live = cfg.materialize(Concurrency.None)
        for (i in -50L..50L) live.update(i)
        // 51 non-negative values survive; HLL estimate should reflect that scale.
        val r = live.read() as HyperLogLogResult
        assertTrue(r.estimate in 30.0..70.0, "estimate=${r.estimate}")
    }

    @Test fun `arithmetic operators build expected ast`() {
        val a: ScalarExpr = 2.0 * X + 1.0
        assertEquals(Add(Mul(Const(2.0), X), Const(1.0)), a)
        val b: ScalarExpr = X / 2.0 - 0.5
        assertEquals(Sub(Div(X, Const(2.0)), Const(0.5)), b)
        val c: ScalarExpr = -X
        assertEquals(Neg(X), c)
    }

    @Test fun `comparison infix builds BoolExpr`() {
        val p = X gt 0.0
        assertEquals(Gt(X, Const(0.0)), p)
        val q = (X gt 0.0) and (X lt 1.0)
        assertEquals(And(Gt(X, Const(0.0)), Lt(X, Const(1.0))), q)
        val r = !(X gt 10.0)
        assertEquals(Not(Gt(X, Const(10.0))), r)
    }

    @Test fun `dsl round trips via SchemaJson`() {
        val expr: ScalarExpr = 2.0 * X + 1.0
        val json = SchemaJson.encodeToString(ScalarExpr.serializer(), expr)
        val decoded = SchemaJson.decodeFromString(ScalarExpr.serializer(), json)
        assertEquals(7.0, decoded.eval(3.0), DELTA)
    }

    @Test fun `transformPair swaps x and y`() {
        val cfg: PairedStatSpec<*> = UnivariateRegression().transformPair(xExpr = Y, yExpr = X)
        val live = cfg.materialize(Concurrency.None)
        // After swap of (1,2),(2,4),(3,6): slope x/y = 0.5
        live.update(1.0, 2.0)
        live.update(2.0, 4.0)
        live.update(3.0, 6.0)
        val r = live.read() as UnivariateRegressionResult
        assertEquals(0.5, r.slope, DELTA)
    }

    @Test fun `transformX only remaps x`() {
        val cfg: PairedStatSpec<*> = UnivariateRegression().transformX(2.0 * X)
        val live = cfg.materialize(Concurrency.None)
        // y=2x with x'=2x gives pairs (2,2),(4,4),(6,6) -> slope 1
        live.update(1.0, 2.0)
        live.update(2.0, 4.0)
        live.update(3.0, 6.0)
        val r = live.read() as UnivariateRegressionResult
        assertEquals(1.0, r.slope, DELTA)
    }

    @Test fun `filter paired drops by predicate over x and y`() {
        val cfg: PairedStatSpec<*> = UnivariateRegression().filter((X gt 0.0) and (Y gt 0.0))
        val live = cfg.materialize(Concurrency.None)
        live.update(-1.0, 5.0)
        live.update(1.0, -5.0)
        live.update(1.0, 2.0)
        live.update(2.0, 4.0)
        live.update(3.0, 6.0)
        val r = live.read() as UnivariateRegressionResult
        assertEquals(2.0, r.slope, DELTA)
    }

    @Test fun `transformElement applies expr per index`() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 3)
            .transformElement(2.0 * X + 1.0)
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(1.0, 2.0, 3.0))
        live.update(doubleArrayOf(4.0, 5.0, 6.0))
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as ResultList<SumResult>
        assertEquals(12.0, rl.results[0].sum, DELTA)
        assertEquals(16.0, rl.results[1].sum, DELTA)
        assertEquals(20.0, rl.results[2].sum, DELTA)
    }

    @Test fun `transformElement can reference other indices`() {
        // Each element divided by index 0.
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2)
            .transformElement(X / V(0))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(2.0, 4.0))
        live.update(doubleArrayOf(5.0, 10.0))
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as ResultList<SumResult>
        assertEquals(2.0, rl.results[0].sum, DELTA)
        assertEquals(4.0, rl.results[1].sum, DELTA)
    }

    @Test fun `filter vector drops by index predicate`() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2)
            .filter(V(0) gt 0.0)
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(-1.0, 100.0))
        live.update(doubleArrayOf(1.0, 10.0))
        live.update(doubleArrayOf(2.0, 20.0))
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as ResultList<SumResult>
        assertEquals(3.0, rl.results[0].sum, DELTA)
        assertEquals(30.0, rl.results[1].sum, DELTA)
    }

    @Test fun `schema vector filter fold and transform borrow custom input`() {
        val input = NoMaterializeVector(doubleArrayOf(2.0, 3.0))
        val filtered = Sum.vectorized(dimensions = 2).filter(V(0) gt 0.0).materialize(Concurrency.None)
        val folded = Sum.foldVector(V(0) + V(1)).materialize(Concurrency.None)
        val transformed = Sum.vectorized(
            dimensions = 2,
        ).transformVector(vectorOf(V(1), V(0))).materialize(Concurrency.None)

        filtered.update(input)
        folded.update(input)
        transformed.update(input)

        assertEquals(2.0, filtered.read().results[0].sum, DELTA)
        assertEquals(5.0, folded.read().sum, DELTA)
        assertEquals(3.0, transformed.read().results[0].sum, DELTA)
    }

    @Test fun `schema regression fold and weight expressions borrow custom input`() {
        val input = NoMaterializeVector(doubleArrayOf(2.0, 3.0))
        val folded = Sum.foldRegression(featureSize = 2, project = V(0) + V(1)).materialize(Concurrency.None)
        val weighted = Sum.foldRegression(featureSize = 2, project = Y)
            .weightBy(V(0))
            .materialize(Concurrency.None)

        folded.update(input, 0.0)
        weighted.update(input, 4.0)

        assertEquals(5.0, folded.read().sum, DELTA)
        assertEquals(8.0, weighted.read().sum, DELTA)
    }

    @Test fun `vfold sum product mean min max norm2`() {
        val v = doubleArrayOf(3.0, -4.0, 0.0)
        assertEquals(-1.0, VFold(VFoldOp.Sum).eval(0.0, 0.0, v), DELTA)
        assertEquals(0.0, VFold(VFoldOp.Product).eval(0.0, 0.0, v), DELTA)
        assertEquals(-1.0 / 3.0, VFold(VFoldOp.Mean).eval(0.0, 0.0, v), DELTA)
        assertEquals(-4.0, VFold(VFoldOp.Min).eval(0.0, 0.0, v), DELTA)
        assertEquals(3.0, VFold(VFoldOp.Max).eval(0.0, 0.0, v), DELTA)
        assertEquals(5.0, VFold(VFoldOp.Norm2).eval(0.0, 0.0, v), DELTA)
    }

    @Test fun `vector aware vfold min and max match double array ordering`() {
        val min = VFold(VFoldOp.Min)
        val max = VFold(VFoldOp.Max)
        val cases = listOf(
            doubleArrayOf(1.0, Double.NaN),
            doubleArrayOf(Double.NaN, 1.0),
            doubleArrayOf(0.0, -0.0),
            doubleArrayOf(-0.0, 0.0),
            doubleArrayOf(3.0, -4.0, 2.0),
        )

        for (values in cases) {
            val input = NoMaterializeVector(values)
            val arrayMin = min.eval(0.0, 0.0, values)
            val arrayMax = max.eval(0.0, 0.0, values)
            val vectorMin = min.eval(v = input)
            val vectorMax = max.eval(v = input)
            assertEquals(arrayMin.toBits(), vectorMin.toBits())
            assertEquals(arrayMax.toBits(), vectorMax.toBits())
        }
    }

    @Test fun `vector aware vfold min and max preserve empty exceptions`() {
        val input = NoMaterializeVector(doubleArrayOf())

        assertFailsWith<IllegalArgumentException> { VFold(VFoldOp.Min).eval(v = input) }
        assertFailsWith<IllegalArgumentException> { VFold(VFoldOp.Max).eval(v = input) }
    }

    @Test fun `vector reductions match double arrays across storage representations`() {
        val values = listOf(
            doubleArrayOf(),
            doubleArrayOf(-0.0),
            doubleArrayOf(0.0, 3.0, -4.0, 0.0),
            doubleArrayOf(Double.NaN, 0.0, 2.0),
            doubleArrayOf(1.0, 0.0, Double.NaN, 2.0),
            doubleArrayOf(0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY),
            doubleArrayOf(-0.0, 0.0, Double.MAX_VALUE, Double.MIN_VALUE),
        )
        val folds = VFoldOp.entries.map(::VFold)

        for (array in values) {
            val representations = listOf<F64VectorLike>(
                F64DenseVector.of(array),
                sparse(array),
                F64StridedVectorView(DoubleArray(array.size * 2) { array[it / 2] }, 0, array.size, 2),
                NoMaterializeVector(array),
            )
            for (fold in folds) {
                for (input in representations) {
                    if (array.isEmpty() && fold.op in setOf(VFoldOp.Mean, VFoldOp.Min, VFoldOp.Max)) {
                        assertFailsWith<IllegalArgumentException> { fold.eval(v = input) }
                    } else {
                        assertEquals(fold.eval(0.0, 0.0, array).toBits(), fold.eval(v = input).toBits())
                    }
                }
            }
        }
    }

    @Test fun `vector vdot matches double arrays for sparse edge cases`() {
        val cases = listOf(
            doubleArrayOf(),
            doubleArrayOf(0.0),
            doubleArrayOf(0.0, 2.0, -3.0, 0.0),
            doubleArrayOf(Double.NaN, 0.0, 2.0),
            doubleArrayOf(0.0, Double.POSITIVE_INFINITY, -0.0),
            doubleArrayOf(Double.MAX_VALUE, Double.MIN_VALUE, 0.0),
        )
        val weightSets = listOf(
            emptyList(),
            listOf(0.0),
            listOf(1.0, 0.0, -1.0, 2.0),
            listOf(1.0, 0.0, -1.0),
            listOf(0.0, 1.0, -1.0),
            listOf(1.0, -1.0, 0.0),
        )

        for ((array, weights) in cases.zip(weightSets)) {
            val expr = VDot(weights)
            val expected = expr.eval(0.0, 0.0, array)
            for (input in listOf<F64VectorLike>(
                F64DenseVector.of(array),
                sparse(array),
                F64StridedVectorView(DoubleArray(array.size * 2) { array[it / 2] }, 0, array.size, 2),
                NoMaterializeVector(array),
            )) {
                assertEquals(expected.toBits(), expr.eval(v = input).toBits())
            }
        }
    }

    @Test fun `vector reductions preserve explicit sparse zeroes`() {
        val array = doubleArrayOf(2.0, 0.0, -3.0)
        val input = F64SparseVector.of(3, intArrayOf(0, 1, 2), array)

        for (op in VFoldOp.entries) {
            val fold = VFold(op)
            assertEquals(fold.eval(0.0, 0.0, array).toBits(), fold.eval(v = input).toBits())
        }
    }

    @Test fun `vector vdot preserves non finite implicit zero behavior`() {
        val values = doubleArrayOf(0.0, 2.0, 0.0)
        val expr = VDot(listOf(Double.POSITIVE_INFINITY, 1.0, Double.NaN))

        assertEquals(expr.eval(0.0, 0.0, values).toBits(), expr.eval(v = sparse(values)).toBits())
    }

    @Test fun `vector vdot preserves length mismatch and zero weights`() {
        assertFailsWith<IllegalArgumentException> { VDot(listOf(1.0)).eval(v = sparse(doubleArrayOf(1.0, 2.0))) }
        assertEquals(0.0, VDot(listOf(0.0, 0.0)).eval(v = sparse(doubleArrayOf(3.0, -4.0))))
    }

    @Test fun `vector aware in evaluates its operand once and matches double array equality`() {
        val empty = In(V(1), emptyList())
        assertFailsWith<IndexOutOfBoundsException> { empty.eval(v = NoMaterializeVector(doubleArrayOf(1.0))) }

        for (values in listOf(doubleArrayOf(Double.NaN), doubleArrayOf(0.0), doubleArrayOf(-0.0))) {
            val expr = In(V(0), listOf(0.0, -0.0, Double.NaN))
            assertEquals(expr.eval(0.0, 0.0, values), expr.eval(v = NoMaterializeVector(values)))
        }

        class CountingVector(private val values: DoubleArray) : F64VectorLike {
            var reads = 0
            override val size: Int get() = values.size
            override fun get(i: Int): Double = values[i].also { reads++ }
            override fun toDoubleArray(): DoubleArray = error("expression materialised its vector input")
        }

        val input = CountingVector(doubleArrayOf(2.0, 3.0))
        assertTrue(In(VFold(VFoldOp.Sum), listOf(1.0, 4.0, 5.0)).eval(v = input))
        assertEquals(2, input.reads)
    }

    @Test fun `vdot weighted dot product`() {
        val v = doubleArrayOf(2.0, 3.0, 4.0)
        val expr = VDot(weights = listOf(1.0, 0.0, -1.0))
        assertEquals(-2.0, expr.eval(0.0, 0.0, v), DELTA)
    }

    @Test fun `vdot length mismatch throws`() {
        assertFailsWith<IllegalArgumentException> {
            VDot(weights = listOf(1.0, 1.0)).eval(0.0, 0.0, doubleArrayOf(1.0, 2.0, 3.0))
        }
    }

    private fun sparse(values: DoubleArray): F64SparseVector {
        val indices = values.indices.filter {
            values[it] != 0.0 || values[it].toBits() == (-0.0).toBits() ||
                values[it].isNaN()
        }
        return F64SparseVector.of(values.size, indices.toIntArray(), DoubleArray(indices.size) { values[indices[it]] })
    }

    @Test fun `foldPaired lifts series to paired with xy expression`() {
        val cfg: PairedStatSpec<*> = Sum.foldPaired(X * Y)
        val live = cfg.materialize(Concurrency.None)
        live.update(1.0, 2.0)
        live.update(3.0, 4.0)
        live.update(5.0, 6.0)
        val r = live.read() as SumResult
        assertEquals(44.0, r.sum, DELTA)
    }

    @Test fun `foldVector with vfold sum`() {
        val cfg: VectorStatSpec<*> = Sum.foldVector(VFold(VFoldOp.Sum))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(1.0, 2.0, 3.0))
        live.update(doubleArrayOf(4.0, 5.0, 6.0))
        val r = live.read() as SumResult
        assertEquals(21.0, r.sum, DELTA)
    }

    @Test fun `foldVector with vdot weighted`() {
        val cfg: VectorStatSpec<*> = Sum.foldVector(VDot(listOf(1.0, 2.0, 3.0)))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(1.0, 1.0, 1.0))
        live.update(doubleArrayOf(2.0, 0.0, 1.0))
        val r = live.read() as SumResult
        assertEquals(11.0, r.sum, DELTA)
    }

    @Test fun `foldVector norm2 drives inner mean`() {
        val cfg: VectorStatSpec<*> = Mean.foldVector(VFold(VFoldOp.Norm2))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(3.0, 4.0))
        live.update(doubleArrayOf(0.0, 0.0))
        val r = live.read() as WeightedMeanResult
        assertEquals(2.5, r.mean, DELTA)
    }

    @Test fun `schema paired vector fold borrows custom input`() {
        val live = UnivariateRegression().foldVector(V(0), V(1)).materialize(Concurrency.None)

        live.update(NoMaterializeVector(doubleArrayOf(1.0, 2.0)))
        live.update(NoMaterializeVector(doubleArrayOf(2.0, 4.0)))

        val result = live.read()
        assertEquals(1.5, result.x.mean, DELTA)
        assertEquals(3.0, result.y.mean, DELTA)
        assertEquals(2.0, result.slope, DELTA)
    }

    @Test fun `fold configs round trip via wire`() {
        val a: PairedStatSpec<*> = Sum.foldPaired(X * Y)
        val ja = SchemaJson.encodeToString(StatSpec.serializer(), a)
        val da = SchemaJson.decodeFromString(StatSpec.serializer(), ja) as FoldPaired
        assertEquals(Mul(X, Y), da.expr)

        val b: VectorStatSpec<*> = Sum.foldVector(VFold(VFoldOp.Sum))
        val jb = SchemaJson.encodeToString(StatSpec.serializer(), b)
        val db = SchemaJson.decodeFromString(StatSpec.serializer(), jb) as FoldVector
        assertEquals(VFold(VFoldOp.Sum), db.expr)

        val c: ScalarExpr = VDot(listOf(1.0, 2.0, 3.0))
        val jc = SchemaJson.encodeToString(ScalarExpr.serializer(), c)
        val dc = SchemaJson.decodeFromString(ScalarExpr.serializer(), jc)
        assertEquals(c, dc)
    }

    @Test fun `vElements evaluates each in order`() {
        val expr = VElements(listOf(V(2), V(0), V(1)))
        val out = expr.eval(0.0, 0.0, doubleArrayOf(10.0, 20.0, 30.0))
        assertEquals(listOf(30.0, 10.0, 20.0), out.toList())
    }

    @Test fun `vElements pools pairs into means`() {
        val expr = VElements(listOf((V(0) + V(1)) / 2.0, (V(2) + V(3)) / 2.0))
        val out = expr.eval(0.0, 0.0, doubleArrayOf(2.0, 4.0, 10.0, 30.0))
        assertEquals(listOf(3.0, 20.0), out.toList())
    }

    @Test fun `transformVector changes dimensionality via VElements`() {
        // Input dim 4 pooled to output dim 2; inner is sized for the output.
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2)
            .transformVector(VElements(listOf((V(0) + V(1)) / 2.0, (V(2) + V(3)) / 2.0)))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(2.0, 4.0, 10.0, 30.0))
        live.update(doubleArrayOf(0.0, 0.0, 4.0, 6.0))
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as ResultList<SumResult>
        assertEquals(3.0, rl.results[0].sum, DELTA)
        assertEquals(25.0, rl.results[1].sum, DELTA)
    }

    @Test fun `transformVector permutes`() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 3)
            .transformVector(VElements(listOf(V(2), V(0), V(1))))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(1.0, 10.0, 100.0))
        live.update(doubleArrayOf(2.0, 20.0, 200.0))
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as ResultList<SumResult>
        assertEquals(300.0, rl.results[0].sum, DELTA)
        assertEquals(3.0, rl.results[1].sum, DELTA)
        assertEquals(30.0, rl.results[2].sum, DELTA)
    }

    @Test fun `transformVector round trips via wire`() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2)
            .transformVector(VElements(listOf(V(0) + V(1), V(0) - V(1))))
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as TransformVector
        val materialized = decoded.materialize(Concurrency.None)
        materialized.update(doubleArrayOf(3.0, 1.0))
        @Suppress("UNCHECKED_CAST")
        val rl = materialized.read() as ResultList<SumResult>
        assertEquals(4.0, rl.results[0].sum, DELTA)
        assertEquals(2.0, rl.results[1].sum, DELTA)
    }

    @Test fun `paired and vector configs round trip via wire`() {
        val cfg: PairedStatSpec<*> = UnivariateRegression().transformPair(xExpr = Y, yExpr = X)
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as TransformPair
        assertEquals(Y, decoded.xExpr)
        assertEquals(X, decoded.yExpr)

        val v: VectorStatSpec<*> = Sum.vectorized(dimensions = 3).filter(V(0) gt 0.0)
        val vJson = SchemaJson.encodeToString(StatSpec.serializer(), v)
        val vDecoded = SchemaJson.decodeFromString(StatSpec.serializer(), vJson) as FilterVector
        assertEquals(Gt(V(0), Const(0.0)), vDecoded.pred)
    }
}
