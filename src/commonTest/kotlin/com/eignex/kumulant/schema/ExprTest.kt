package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class ExprTest {

    // ===== ScalarExpr eval =====

    @Test fun x_returns_input() {
        assertEquals(3.0, X.eval(3.0), DELTA)
    }

    @Test fun const_ignores_input() {
        assertEquals(7.0, Const(7.0).eval(99.0), DELTA)
    }

    @Test fun add_sub_mul_div() {
        assertEquals(5.0, Add(X, Const(2.0)).eval(3.0), DELTA)
        assertEquals(1.0, Sub(X, Const(2.0)).eval(3.0), DELTA)
        assertEquals(6.0, Mul(X, Const(2.0)).eval(3.0), DELTA)
        assertEquals(1.5, Div(X, Const(2.0)).eval(3.0), DELTA)
    }

    @Test fun neg_abs() {
        assertEquals(-3.0, Neg(X).eval(3.0), DELTA)
        assertEquals(3.0, Abs(X).eval(-3.0), DELTA)
    }

    @Test fun log_exp_sqrt_pow() {
        assertEquals(0.0, Log(Const(1.0)).eval(99.0), DELTA)
        assertEquals(1.0, Exp(Const(0.0)).eval(99.0), DELTA)
        assertEquals(3.0, Sqrt(Const(9.0)).eval(99.0), DELTA)
        assertEquals(8.0, Pow(Const(2.0), Const(3.0)).eval(99.0), DELTA)
    }

    @Test fun min_max() {
        assertEquals(2.0, MinExpr(X, Const(2.0)).eval(5.0), DELTA)
        assertEquals(5.0, MaxExpr(X, Const(2.0)).eval(5.0), DELTA)
    }

    @Test fun if_branches() {
        val expr = IfExpr(Gt(X, Const(0.0)), Const(1.0), Const(-1.0))
        assertEquals(1.0, expr.eval(0.5), DELTA)
        assertEquals(-1.0, expr.eval(-0.5), DELTA)
    }

    @Test fun composed_2x_plus_1() {
        val expr = Add(Mul(Const(2.0), X), Const(1.0))
        assertEquals(7.0, expr.eval(3.0), DELTA)
    }

    @Test fun clip_via_min_max() {
        val clip = MinExpr(Const(10.0), MaxExpr(Const(0.0), X))
        assertEquals(0.0, clip.eval(-5.0), DELTA)
        assertEquals(5.0, clip.eval(5.0), DELTA)
        assertEquals(10.0, clip.eval(20.0), DELTA)
    }

    // ===== BoolExpr eval =====

    @Test fun gt_lt_ge_le_eq() {
        assertEquals(true, Gt(X, Const(0.0)).eval(1.0))
        assertEquals(false, Gt(X, Const(0.0)).eval(0.0))
        assertEquals(true, Ge(X, Const(0.0)).eval(0.0))
        assertEquals(true, Lt(X, Const(0.0)).eval(-1.0))
        assertEquals(true, Le(X, Const(0.0)).eval(0.0))
        assertEquals(true, Eq(X, Const(5.0)).eval(5.0))
    }

    @Test fun and_or_not() {
        val a = Gt(X, Const(0.0))
        val b = Lt(X, Const(10.0))
        assertEquals(true, And(a, b).eval(5.0))
        assertEquals(false, And(a, b).eval(15.0))
        assertEquals(true, Or(a, b).eval(15.0))
        assertEquals(false, Not(a).eval(5.0))
    }

    @Test fun in_range_inclusive() {
        val r = InRange(X, 0.0, 10.0)
        assertEquals(true, r.eval(0.0))
        assertEquals(true, r.eval(10.0))
        assertEquals(true, r.eval(5.0))
        assertEquals(false, r.eval(-0.001))
        assertEquals(false, r.eval(10.001))
    }

    // ===== Round-trip via SchemaJson =====

    @Test fun scalar_expr_round_trips() {
        val expr: ScalarExpr = Add(Mul(Const(2.0), X), Const(1.0))
        val json = SchemaJson.encodeToString(ScalarExpr.serializer(), expr)
        val decoded = SchemaJson.decodeFromString(ScalarExpr.serializer(), json)
        assertEquals(expr, decoded)
        assertEquals(7.0, decoded.eval(3.0), DELTA)
    }

    @Test fun bool_expr_round_trips() {
        val pred: BoolExpr = And(Gt(X, Const(0.0)), Lt(X, Const(1.0)))
        val json = SchemaJson.encodeToString(BoolExpr.serializer(), pred)
        val decoded = SchemaJson.decodeFromString(BoolExpr.serializer(), json)
        assertEquals(pred, decoded)
        assertEquals(true, decoded.eval(0.5))
        assertEquals(false, decoded.eval(2.0))
    }

    @Test fun if_with_bool_round_trips() {
        val expr: ScalarExpr = IfExpr(InRange(X, 0.0, 1.0), X, Const(0.0))
        val json = SchemaJson.encodeToString(ScalarExpr.serializer(), expr)
        val decoded = SchemaJson.decodeFromString(ScalarExpr.serializer(), json)
        assertEquals(expr, decoded)
        assertEquals(0.5, decoded.eval(0.5), DELTA)
        assertEquals(0.0, decoded.eval(2.0), DELTA)
    }

    // ===== Transform / Filter configs =====

    @Test fun transform_series_applies_expr_per_update() {
        val cfg: SeriesStatSpec<SumResult> =
            Sum.transform(Add(Mul(Const(2.0), X), Const(1.0))) // 2x + 1
        val live = cfg.materialize(Concurrency.None)
        live.update(1.0)
        live.update(2.0)
        live.update(3.0)
        // (2*1+1) + (2*2+1) + (2*3+1) = 3 + 5 + 7 = 15
        assertEquals(15.0, live.read().sum, DELTA)
    }

    @Test fun filter_series_drops_non_matching_updates() {
        val cfg: SeriesStatSpec<SumResult> = Sum.filter(Gt(X, Const(0.0)))
        val live = cfg.materialize(Concurrency.None)
        live.update(-1.0)
        live.update(2.0)
        live.update(-3.0)
        live.update(4.0)
        assertEquals(6.0, live.read().sum, DELTA)
    }

    @Test fun transform_chained_with_other_ops() {
        val cfg: SeriesStatSpec<SumResult> =
            Sum.transform(MinExpr(Const(10.0), MaxExpr(Const(0.0), X))) // clip 0..10
                .withWeight(2.0)
        val live = cfg.materialize(Concurrency.None)
        live.update(-5.0)
        live.update(5.0)
        live.update(20.0)
        // clipped: 0, 5, 10 → weights 2 each → sum = 0+10+20 = 30
        assertEquals(30.0, live.read().sum, DELTA)
    }

    @Test fun transform_config_round_trips() {
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

    @Test fun filter_discrete_drops_non_matching_long_updates() {
        val cfg: DiscreteStatSpec<*> = HyperLogLog(precision = 10).filter(Ge(X, Const(0.0)))
        val live = cfg.materialize(Concurrency.None)
        for (i in -50L..50L) live.update(i)
        // Only non-negative survive — 51 distinct values; HLL estimate should reflect that scale.
        val r = live.read() as com.eignex.kumulant.stat.cardinality.HyperLogLogResult
        kotlin.test.assertTrue(r.estimate in 30.0..70.0, "estimate=${r.estimate}")
    }

    // ===== DSL operator overloads =====

    @Test fun arithmetic_operators_build_expected_ast() {
        val a: ScalarExpr = 2.0 * X + 1.0
        assertEquals(Add(Mul(Const(2.0), X), Const(1.0)), a)
        val b: ScalarExpr = X / 2.0 - 0.5
        assertEquals(Sub(Div(X, Const(2.0)), Const(0.5)), b)
        val c: ScalarExpr = -X
        assertEquals(Neg(X), c)
    }

    @Test fun comparison_infix_builds_BoolExpr() {
        val p = X gt 0.0
        assertEquals(Gt(X, Const(0.0)), p)
        val q = (X gt 0.0) and (X lt 1.0)
        assertEquals(And(Gt(X, Const(0.0)), Lt(X, Const(1.0))), q)
        val r = !(X gt 10.0)
        assertEquals(Not(Gt(X, Const(10.0))), r)
    }

    @Test fun dsl_round_trips_via_SchemaJson() {
        val expr: ScalarExpr = 2.0 * X + 1.0
        val json = SchemaJson.encodeToString(ScalarExpr.serializer(), expr)
        val decoded = SchemaJson.decodeFromString(ScalarExpr.serializer(), json)
        assertEquals(7.0, decoded.eval(3.0), DELTA)
    }

    // ===== Paired transforms =====

    @Test fun transformPair_swaps_x_and_y() {
        val cfg: PairedStatSpec<*> = OLS.transformPair(xExpr = Y, yExpr = X)
        val live = cfg.materialize(Concurrency.None)
        // After swap, slope between (originally x=1,y=2),(2,4),(3,6) becomes y/x → 0.5
        live.update(1.0, 2.0)
        live.update(2.0, 4.0)
        live.update(3.0, 6.0)
        val r = live.read() as com.eignex.kumulant.stat.regression.OLSResult
        assertEquals(0.5, r.slope, DELTA)
    }

    @Test fun transformX_only_remaps_x() {
        val cfg: PairedStatSpec<*> = OLS.transformX(2.0 * X)
        val live = cfg.materialize(Concurrency.None)
        // Original: y = 2x; after x' = 2x: pairs (2,2),(4,4),(6,6) → slope 1.
        live.update(1.0, 2.0)
        live.update(2.0, 4.0)
        live.update(3.0, 6.0)
        val r = live.read() as com.eignex.kumulant.stat.regression.OLSResult
        assertEquals(1.0, r.slope, DELTA)
    }

    @Test fun filter_paired_drops_by_predicate_over_x_and_y() {
        val cfg: PairedStatSpec<*> = OLS.filter((X gt 0.0) and (Y gt 0.0))
        val live = cfg.materialize(Concurrency.None)
        live.update(-1.0, 5.0) // dropped (x <= 0)
        live.update(1.0, -5.0) // dropped (y <= 0)
        live.update(1.0, 2.0)
        live.update(2.0, 4.0)
        live.update(3.0, 6.0)
        val r = live.read() as com.eignex.kumulant.stat.regression.OLSResult
        assertEquals(2.0, r.slope, DELTA)
    }

    // ===== Vector transforms =====

    @Test fun transformElement_applies_expr_per_index() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 3)
            .transformElement(2.0 * X + 1.0)
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(1.0, 2.0, 3.0))
        live.update(doubleArrayOf(4.0, 5.0, 6.0))
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as com.eignex.kumulant.core.ResultList<SumResult>
        // After transform: (3, 5, 7) and (9, 11, 13). SumStat per dim: 12, 16, 20.
        assertEquals(12.0, rl.results[0].sum, DELTA)
        assertEquals(16.0, rl.results[1].sum, DELTA)
        assertEquals(20.0, rl.results[2].sum, DELTA)
    }

    @Test fun transformElement_can_reference_other_indices() {
        // L1-ish normalization: each element divided by index 0.
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2)
            .transformElement(X / V(0))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(2.0, 4.0)) // -> 1.0, 2.0
        live.update(doubleArrayOf(5.0, 10.0)) // -> 1.0, 2.0
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as com.eignex.kumulant.core.ResultList<SumResult>
        assertEquals(2.0, rl.results[0].sum, DELTA)
        assertEquals(4.0, rl.results[1].sum, DELTA)
    }

    @Test fun filter_vector_drops_by_index_predicate() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2)
            .filter(V(0) gt 0.0)
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(-1.0, 100.0)) // dropped
        live.update(doubleArrayOf(1.0, 10.0))
        live.update(doubleArrayOf(2.0, 20.0))
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as com.eignex.kumulant.core.ResultList<SumResult>
        assertEquals(3.0, rl.results[0].sum, DELTA)
        assertEquals(30.0, rl.results[1].sum, DELTA)
    }

    // ===== VFold / VDot reduction nodes =====

    @Test fun vfold_sum_product_mean_min_max_norm2() {
        val v = doubleArrayOf(3.0, -4.0, 0.0)
        assertEquals(-1.0, VFold(VFoldOp.SumStat).eval(0.0, 0.0, v), DELTA)
        assertEquals(0.0, VFold(VFoldOp.Product).eval(0.0, 0.0, v), DELTA)
        assertEquals(-1.0 / 3.0, VFold(VFoldOp.MeanStat).eval(0.0, 0.0, v), DELTA)
        assertEquals(-4.0, VFold(VFoldOp.MinStat).eval(0.0, 0.0, v), DELTA)
        assertEquals(3.0, VFold(VFoldOp.MaxStat).eval(0.0, 0.0, v), DELTA)
        assertEquals(5.0, VFold(VFoldOp.Norm2).eval(0.0, 0.0, v), DELTA) // sqrt(9 + 16)
    }

    @Test fun vdot_weighted_dot_product() {
        val v = doubleArrayOf(2.0, 3.0, 4.0)
        val expr = VDot(weights = listOf(1.0, 0.0, -1.0))
        assertEquals(-2.0, expr.eval(0.0, 0.0, v), DELTA)
    }

    @Test fun vdot_length_mismatch_throws() {
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            VDot(weights = listOf(1.0, 1.0)).eval(0.0, 0.0, doubleArrayOf(1.0, 2.0, 3.0))
        }
    }

    // ===== FoldPaired / FoldVector configs =====

    @Test fun foldPaired_lifts_series_to_paired_with_xy_expression() {
        val cfg: PairedStatSpec<*> = Sum.foldPaired(X * Y)
        val live = cfg.materialize(Concurrency.None)
        live.update(1.0, 2.0) // 2
        live.update(3.0, 4.0) // 12
        live.update(5.0, 6.0) // 30
        val r = live.read() as SumResult
        assertEquals(44.0, r.sum, DELTA)
    }

    @Test fun foldVector_with_vfold_sum() {
        val cfg: VectorStatSpec<*> = Sum.foldVector(VFold(VFoldOp.SumStat))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(1.0, 2.0, 3.0)) // sum = 6
        live.update(doubleArrayOf(4.0, 5.0, 6.0)) // sum = 15
        val r = live.read() as SumResult
        assertEquals(21.0, r.sum, DELTA)
    }

    @Test fun foldVector_with_vdot_weighted() {
        val cfg: VectorStatSpec<*> = Sum.foldVector(VDot(listOf(1.0, 2.0, 3.0)))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(1.0, 1.0, 1.0)) // 1+2+3 = 6
        live.update(doubleArrayOf(2.0, 0.0, 1.0)) // 2+0+3 = 5
        val r = live.read() as SumResult
        assertEquals(11.0, r.sum, DELTA)
    }

    @Test fun foldVector_norm2_drives_inner_mean() {
        val cfg: VectorStatSpec<*> = Mean.foldVector(VFold(VFoldOp.Norm2))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(3.0, 4.0)) // norm = 5
        live.update(doubleArrayOf(0.0, 0.0)) // norm = 0
        val r = live.read() as com.eignex.kumulant.stat.summary.WeightedMeanResult
        assertEquals(2.5, r.mean, DELTA)
    }

    @Test fun fold_configs_round_trip_via_wire() {
        val a: PairedStatSpec<*> = Sum.foldPaired(X * Y)
        val ja = SchemaJson.encodeToString(StatSpec.serializer(), a)
        val da = SchemaJson.decodeFromString(StatSpec.serializer(), ja) as FoldPaired
        assertEquals(Mul(X, Y), da.expr)

        val b: VectorStatSpec<*> = Sum.foldVector(VFold(VFoldOp.SumStat))
        val jb = SchemaJson.encodeToString(StatSpec.serializer(), b)
        val db = SchemaJson.decodeFromString(StatSpec.serializer(), jb) as FoldVector
        assertEquals(VFold(VFoldOp.SumStat), db.expr)

        val c: ScalarExpr = VDot(listOf(1.0, 2.0, 3.0))
        val jc = SchemaJson.encodeToString(ScalarExpr.serializer(), c)
        val dc = SchemaJson.decodeFromString(ScalarExpr.serializer(), jc)
        assertEquals(c, dc)
    }

    // ===== VectorExpr — vector→vector transforms =====

    @Test fun vElements_evaluates_each_in_order() {
        val expr = VElements(listOf(V(2), V(0), V(1))) // permute
        val out = expr.eval(0.0, 0.0, doubleArrayOf(10.0, 20.0, 30.0))
        kotlin.test.assertEquals(listOf(30.0, 10.0, 20.0), out.toList())
    }

    @Test fun vElements_pools_pairs_into_means() {
        val expr = VElements(listOf((V(0) + V(1)) / 2.0, (V(2) + V(3)) / 2.0))
        val out = expr.eval(0.0, 0.0, doubleArrayOf(2.0, 4.0, 10.0, 30.0))
        kotlin.test.assertEquals(listOf(3.0, 20.0), out.toList())
    }

    @Test fun transformVector_changes_dimensionality_via_VElements() {
        // Input dim 4 → pooled output dim 2. Inner is sized for the output.
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2)
            .transformVector(VElements(listOf((V(0) + V(1)) / 2.0, (V(2) + V(3)) / 2.0)))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(2.0, 4.0, 10.0, 30.0)) // → (3, 20)
        live.update(doubleArrayOf(0.0, 0.0, 4.0, 6.0)) // → (0, 5)
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as com.eignex.kumulant.core.ResultList<SumResult>
        assertEquals(3.0, rl.results[0].sum, DELTA)
        assertEquals(25.0, rl.results[1].sum, DELTA)
    }

    @Test fun transformVector_permutes() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 3)
            .transformVector(VElements(listOf(V(2), V(0), V(1))))
        val live = cfg.materialize(Concurrency.None)
        live.update(doubleArrayOf(1.0, 10.0, 100.0))
        live.update(doubleArrayOf(2.0, 20.0, 200.0))
        @Suppress("UNCHECKED_CAST")
        val rl = live.read() as com.eignex.kumulant.core.ResultList<SumResult>
        assertEquals(300.0, rl.results[0].sum, DELTA) // was V(2)
        assertEquals(3.0, rl.results[1].sum, DELTA) // was V(0)
        assertEquals(30.0, rl.results[2].sum, DELTA) // was V(1)
    }

    @Test fun transformVector_round_trips_via_wire() {
        val cfg: VectorStatSpec<*> = Sum.vectorized(dimensions = 2)
            .transformVector(VElements(listOf(V(0) + V(1), V(0) - V(1))))
        val json = SchemaJson.encodeToString(StatSpec.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json) as TransformVector
        val materialized = decoded.materialize(Concurrency.None)
        materialized.update(doubleArrayOf(3.0, 1.0)) // → (4, 2)
        @Suppress("UNCHECKED_CAST")
        val rl = materialized.read() as com.eignex.kumulant.core.ResultList<SumResult>
        assertEquals(4.0, rl.results[0].sum, DELTA)
        assertEquals(2.0, rl.results[1].sum, DELTA)
    }

    @Test fun paired_and_vector_configs_round_trip_via_wire() {
        val cfg: PairedStatSpec<*> = OLS.transformPair(xExpr = Y, yExpr = X)
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
