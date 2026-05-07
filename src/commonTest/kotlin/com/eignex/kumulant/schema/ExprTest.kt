package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals

class ExprTest {

    private val DELTA = 1e-12

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
        val cfg: SeriesStatConfig<SumResult> =
            SumConfig.transform(Add(Mul(Const(2.0), X), Const(1.0)))  // 2x + 1
        val live = cfg.materialize(Concurrency.None)
        live.update(1.0); live.update(2.0); live.update(3.0)
        // (2*1+1) + (2*2+1) + (2*3+1) = 3 + 5 + 7 = 15
        assertEquals(15.0, live.read().sum, DELTA)
    }

    @Test fun filter_series_drops_non_matching_updates() {
        val cfg: SeriesStatConfig<SumResult> = SumConfig.filter(Gt(X, Const(0.0)))
        val live = cfg.materialize(Concurrency.None)
        live.update(-1.0); live.update(2.0); live.update(-3.0); live.update(4.0)
        assertEquals(6.0, live.read().sum, DELTA)
    }

    @Test fun transform_chained_with_other_ops() {
        val cfg: SeriesStatConfig<SumResult> =
            SumConfig.transform(MinExpr(Const(10.0), MaxExpr(Const(0.0), X)))  // clip 0..10
                .withWeight(2.0)
        val live = cfg.materialize(Concurrency.None)
        live.update(-5.0); live.update(5.0); live.update(20.0)
        // clipped: 0, 5, 10 → weights 2 each → sum = 0+10+20 = 30
        assertEquals(30.0, live.read().sum, DELTA)
    }

    @Test fun transform_config_round_trips() {
        val cfg: SeriesStatConfig<SumResult> =
            SumConfig.transform(MinExpr(Const(10.0), MaxExpr(Const(0.0), X)))
        val json = SchemaJson.encodeToString(StatConfig.serializer(), cfg)
        val decoded = SchemaJson.decodeFromString(StatConfig.serializer(), json)
        val live = (decoded as SeriesStatConfig<*>).materialize(Concurrency.None)
        live.update(-1.0); live.update(5.0); live.update(20.0)
        val sum = (live.read() as SumResult).sum
        assertEquals(15.0, sum, DELTA)
    }

    @Test fun filter_discrete_drops_non_matching_long_updates() {
        val cfg: DiscreteStatConfig<*> = HyperLogLogConfig(precision = 10).filter(Ge(X, Const(0.0)))
        val live = cfg.materialize(Concurrency.None)
        for (i in -50L..50L) live.update(i)
        // Only non-negative survive — 51 distinct values; HLL estimate should reflect that scale.
        val r = live.read() as com.eignex.kumulant.stat.cardinality.HyperLogLogResult
        kotlin.test.assertTrue(r.estimate in 30.0..70.0, "estimate=${r.estimate}")
    }
}
