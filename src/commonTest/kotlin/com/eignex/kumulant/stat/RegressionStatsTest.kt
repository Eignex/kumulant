package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.AtomicMode
import com.eignex.kumulant.concurrent.SerialMode
import kotlin.math.sqrt
import kotlin.test.*

private const val EPS = 1e-9
private const val APPROX = 1e-6

class OLSTest {

    @Test
    fun `perfect positive line y = 2x + 1`() {
        val ols = OLS()
        for (x in 0..9) ols.update(x.toDouble(), 2.0 * x + 1.0)
        val r = ols.read()
        assertEquals(2.0, r.slope, APPROX)
        assertEquals(1.0, r.intercept, APPROX)
    }

    @Test
    fun `perfect negative line y = -3x + 5`() {
        val ols = OLS()
        for (x in 0..9) ols.update(x.toDouble(), -3.0 * x + 5.0)
        val r = ols.read()
        assertEquals(-3.0, r.slope, APPROX)
        assertEquals(5.0, r.intercept, APPROX)
    }

    @Test
    fun `horizontal line slope is zero`() {
        val ols = OLS()
        repeat(5) { ols.update(it.toDouble(), 7.0) }
        val r = ols.read()
        assertEquals(0.0, r.slope, APPROX)
        assertEquals(7.0, r.intercept, APPROX)
    }

    @Test
    fun `vertical data all same x gives slope zero`() {
        val ols = OLS()
        repeat(5) { ols.update(3.0, it.toDouble()) }
        val r = ols.read()
        assertEquals(0.0, r.slope, EPS)
    }

    @Test
    fun `r-squared is 1 for perfect fit`() {
        val ols = OLS()
        for (x in 0..9) ols.update(x.toDouble(), 2.0 * x + 1.0)
        assertEquals(1.0, ols.read().rSquared, APPROX)
    }

    @Test
    fun `r-squared is low for noisy data`() {
        val ols = OLS()

        ols.update(0.0, 10.0)
        ols.update(1.0, -5.0)
        ols.update(2.0, 8.0)
        ols.update(3.0, -3.0)
        assertTrue(ols.read().rSquared < 0.5)
    }

    @Test
    fun `sse is non-negative`() {
        val ols = OLS()
        repeat(10) { ols.update(it.toDouble(), it * 2.0 + 0.1 * (it % 3 - 1)) }
        assertTrue(ols.read().sse >= 0.0)
    }

    @Test
    fun `correlation sign matches slope sign`() {
        val pos =
            OLS().apply { for (x in 0..9) update(x.toDouble(), x.toDouble()) }
        val neg =
            OLS().apply { for (x in 0..9) update(x.toDouble(), -x.toDouble()) }
        assertTrue(pos.read().correlation > 0.0)
        assertTrue(neg.read().correlation < 0.0)
    }

    @Test
    fun `x and y means are correct`() {
        val ols = OLS()
        for (x in 1..5) ols.update(x.toDouble(), x.toDouble() * 2)
        val r = ols.read()
        assertEquals(3.0, r.x.mean, APPROX)
        assertEquals(6.0, r.y.mean, APPROX)
    }

    @Test
    fun `totalWeights equals number of updates`() {
        val ols = OLS()
        repeat(7) { ols.update(it.toDouble(), it.toDouble()) }
        assertEquals(7.0, ols.read().totalWeights, EPS)
    }

    @Test
    fun `weighted update shifts means`() {
        val ols = OLS()
        ols.update(0.0, 0.0, weight = 1.0)
        ols.update(10.0, 10.0, weight = 9.0)
        val r = ols.read()
        assertEquals(9.0, r.x.mean, APPROX)
    }

    @Test
    fun `merge of two perfect-line halves recovers full regression`() {
        val full =
            OLS().apply { for (x in 0..19) update(x.toDouble(), 3.0 * x + 2.0) }
        val left =
            OLS().apply { for (x in 0..9) update(x.toDouble(), 3.0 * x + 2.0) }
        val right = OLS().apply {
            for (x in 10..19) update(
                x.toDouble(),
                3.0 * x + 2.0
            )
        }
        left.merge(right.read())

        assertEquals(full.read().slope, left.read().slope, APPROX)
        assertEquals(full.read().intercept, left.read().intercept, APPROX)
        assertEquals(full.read().rSquared, left.read().rSquared, APPROX)
    }

    @Test
    fun `merge with empty is no-op`() {
        val ols =
            OLS().apply { for (x in 0..4) update(x.toDouble(), x.toDouble()) }
        val empty = OLS()
        val before = ols.read()
        ols.merge(empty.read())
        assertEquals(before.slope, ols.read().slope, EPS)
        assertEquals(before.intercept, ols.read().intercept, EPS)
    }

    @Test
    fun `reset clears all state`() {
        val ols =
            OLS().apply { for (x in 0..9) update(x.toDouble(), x.toDouble()) }
        ols.reset()
        val r = ols.read()
        assertEquals(0.0, r.totalWeights, EPS)
        assertEquals(0.0, r.slope, EPS)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val ols1 = OLS(AtomicMode).apply {
            for (x in 0..4) update(x.toDouble(), x.toDouble())
        }
        val ols2 = ols1.create(SerialMode)
        ols2.update(100.0, 200.0)

        assertEquals(5.0, ols1.read().totalWeights, EPS)
    }

    @Test
    fun `predict uses slope and intercept`() {
        val ols =
            OLS().apply { for (x in 0..9) update(x.toDouble(), 2.0 * x + 1.0) }
        val r = ols.read()
        assertEquals(11.0, r.predict(5.0), APPROX)
    }

    @Test
    fun `mse and rmse derive from sse and totalWeights`() {
        val ols = OLS()
        ols.update(0.0, 10.0)
        ols.update(1.0, -5.0)
        ols.update(2.0, 8.0)
        ols.update(3.0, -3.0)
        val r = ols.read()
        assertEquals(r.sse / r.totalWeights, r.mse, APPROX)
        assertEquals(sqrt(r.mse), r.rmse, APPROX)
        assertTrue(r.mse > 0.0)
    }

    @Test
    fun `ssr plus sse equals sst`() {
        val ols = OLS()
        for (x in 0..9) ols.update(x.toDouble(), 2.0 * x + 1.0 + 0.1 * (x % 3 - 1))
        val r = ols.read()
        assertEquals(r.sst, r.ssr + r.sse, APPROX)
    }

    @Test
    fun `regression metrics are zero for empty stat`() {
        val r = OLS().read()
        assertEquals(0.0, r.totalWeights, EPS)
        assertEquals(0.0, r.sse, EPS)
        assertEquals(0.0, r.sst, EPS)
        assertEquals(0.0, r.mse, EPS)
        assertEquals(0.0, r.rmse, EPS)
        assertEquals(0.0, r.ssr, EPS)
        assertEquals(0.0, r.rSquared, EPS)
    }
}

class CovarianceTest {

    @Test
    fun `covariance of perfectly correlated data`() {
        val cov = Covariance()
        for (i in 1..5) cov.update(i.toDouble(), i.toDouble())
        val r = cov.read()

        assertEquals(2.0, r.covariance, EPS)
        assertEquals(1.0, r.correlation, EPS)
    }

    @Test
    fun `covariance of negatively correlated data`() {
        val cov = Covariance()
        for (i in 1..5) cov.update(i.toDouble(), (6 - i).toDouble())
        val r = cov.read()
        assertEquals(-2.0, r.covariance, EPS)
        assertEquals(-1.0, r.correlation, EPS)
    }

    @Test
    fun `covariance of uncorrelated data`() {
        val cov = Covariance()

        cov.update(1.0, 1.0)
        cov.update(-1.0, -1.0)
        cov.update(1.0, -1.0)
        cov.update(-1.0, 1.0)
        val r = cov.read()
        assertEquals(0.0, r.covariance, EPS)
    }

    @Test
    fun `merge combines two streams correctly`() {
        val c1 = Covariance().apply {
            update(1.0, 2.0)
            update(2.0, 4.0)
        }
        val c2 = Covariance().apply {
            update(3.0, 6.0)
            update(4.0, 8.0)
        }
        c1.merge(c2.read())
        val r = c1.read()

        assertEquals(1.0, r.correlation, APPROX)
    }

    @Test
    fun `reset clears state`() {
        val cov = Covariance().apply {
            update(1.0, 1.0)
            update(2.0, 2.0)
        }
        cov.reset()
        assertEquals(0.0, cov.read().covariance, EPS)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val c1 = Covariance().apply {
            update(1.0, 2.0)
            update(2.0, 4.0)
        }
        val c2 = c1.create()
        c2.update(5.0, 10.0)

        assertEquals(2.0, c1.read().totalWeights, EPS)
        assertEquals(1.0, c2.read().totalWeights, EPS)
    }
}

class RidgeTest {

    @Test
    fun `lambda 0 reproduces OLS slope and intercept`() {
        val ridge = Ridge(lambda = 0.0)
        val ols = OLS()
        for (x in 0..9) {
            val y = 2.0 * x + 1.0
            ridge.update(x.toDouble(), y)
            ols.update(x.toDouble(), y)
        }
        val r = ridge.read()
        val o = ols.read()
        assertEquals(o.slope, r.slope, APPROX)
        assertEquals(o.intercept, r.intercept, APPROX)
    }

    @Test
    fun `positive lambda shrinks slope toward zero`() {
        val unreg = Ridge(0.0).apply {
            for (x in 0..9) update(x.toDouble(), 2.0 * x + 1.0)
        }
        val reg = Ridge(10.0).apply {
            for (x in 0..9) update(x.toDouble(), 2.0 * x + 1.0)
        }
        val u = unreg.read().slope
        val r = reg.read().slope
        assertTrue(r in 0.0..u, "expected 0 < $r < $u")
    }

    @Test
    fun `large lambda drives slope toward zero and intercept toward meanY`() {
        val ridge = Ridge(lambda = 1e9)
        for (x in 0..9) ridge.update(x.toDouble(), 2.0 * x + 1.0)
        val r = ridge.read()
        assertEquals(0.0, r.slope, 1e-6)
        assertEquals(r.y.mean, r.intercept, 1e-6)
    }

    @Test
    fun `sse is non-negative and at least OLS sse`() {
        val ols = OLS()
        val ridge = Ridge(lambda = 5.0)
        for (x in 0..9) {
            val y = 2.0 * x + 1.0 + (x % 3 - 1) * 0.5
            ols.update(x.toDouble(), y)
            ridge.update(x.toDouble(), y)
        }
        val r = ridge.read()
        assertTrue(r.sse >= 0.0)
        assertTrue(r.sse >= ols.read().sse - APPROX)
    }

    @Test
    fun `predict uses ridge slope and intercept`() {
        val ridge = Ridge(lambda = 1.0)
        for (x in 0..9) ridge.update(x.toDouble(), 2.0 * x + 1.0)
        val r = ridge.read()
        assertEquals(r.slope * 5.0 + r.intercept, r.predict(5.0), APPROX)
    }

    @Test
    fun `merge of two halves matches a single accumulator`() {
        val full = Ridge(lambda = 2.0).apply {
            for (x in 0..19) update(x.toDouble(), 3.0 * x + 2.0)
        }
        val left = Ridge(lambda = 2.0).apply {
            for (x in 0..9) update(x.toDouble(), 3.0 * x + 2.0)
        }
        val right = Ridge(lambda = 2.0).apply {
            for (x in 10..19) update(x.toDouble(), 3.0 * x + 2.0)
        }
        left.merge(right.read())

        assertEquals(full.read().slope, left.read().slope, APPROX)
        assertEquals(full.read().intercept, left.read().intercept, APPROX)
        assertEquals(full.read().sse, left.read().sse, APPROX)
    }

    @Test
    fun `reset clears state`() {
        val ridge = Ridge(lambda = 1.0).apply {
            for (x in 0..9) update(x.toDouble(), x.toDouble())
        }
        ridge.reset()
        val r = ridge.read()
        assertEquals(0.0, r.totalWeights, EPS)
        assertEquals(0.0, r.slope, EPS)
    }

    @Test
    fun `create produces fresh independent stat with same lambda`() {
        val r1 = Ridge(lambda = 3.0).apply {
            for (x in 0..4) update(x.toDouble(), x.toDouble())
        }
        val r2 = r1.create()
        r2.update(100.0, 200.0)
        assertEquals(5.0, r1.read().totalWeights, EPS)
        assertEquals(1.0, r2.read().totalWeights, EPS)
        assertEquals(3.0, r2.read().lambda, EPS)
    }
}

class LassoTest {

    @Test
    fun `lambda 0 reproduces OLS slope`() {
        val lasso = Lasso(lambda = 0.0)
        val ols = OLS()
        for (x in 0..9) {
            val y = 2.0 * x + 1.0
            lasso.update(x.toDouble(), y)
            ols.update(x.toDouble(), y)
        }
        assertEquals(ols.read().slope, lasso.read().slope, APPROX)
        assertEquals(ols.read().intercept, lasso.read().intercept, APPROX)
    }

    @Test
    fun `small lambda shrinks slope toward zero but keeps sign`() {
        val unreg = Lasso(0.0).apply {
            for (x in 0..9) update(x.toDouble(), 2.0 * x + 1.0)
        }
        val reg = Lasso(1.0).apply {
            for (x in 0..9) update(x.toDouble(), 2.0 * x + 1.0)
        }
        val u = unreg.read().slope
        val r = reg.read().slope
        assertTrue(u > 0.0)
        assertTrue(r > 0.0 && r < u, "expected 0 < $r < $u")
    }

    @Test
    fun `large lambda zeros slope and sets intercept to meanY`() {
        val lasso = Lasso(lambda = 1e9)
        for (x in 0..9) lasso.update(x.toDouble(), 2.0 * x + 1.0)
        val r = lasso.read()
        assertEquals(0.0, r.slope, EPS)
        assertEquals(r.y.mean, r.intercept, EPS)
    }

    @Test
    fun `predict at slope-zeroing lambda returns meanY`() {
        val lasso = Lasso(lambda = 1e9)
        for (x in 0..9) lasso.update(x.toDouble(), 2.0 * x + 1.0)
        val r = lasso.read()
        assertEquals(r.y.mean, r.predict(42.0), EPS)
    }

    @Test
    fun `merge of two halves matches a single accumulator`() {
        val full = Lasso(lambda = 1.0).apply {
            for (x in 0..19) update(x.toDouble(), 3.0 * x + 2.0)
        }
        val left = Lasso(lambda = 1.0).apply {
            for (x in 0..9) update(x.toDouble(), 3.0 * x + 2.0)
        }
        val right = Lasso(lambda = 1.0).apply {
            for (x in 10..19) update(x.toDouble(), 3.0 * x + 2.0)
        }
        left.merge(right.read())

        assertEquals(full.read().slope, left.read().slope, APPROX)
        assertEquals(full.read().intercept, left.read().intercept, APPROX)
        assertEquals(full.read().sxy, left.read().sxy, APPROX)
    }

    @Test
    fun `merge round-trips when both halves have slope zero under lambda`() {
        // λ huge → both halves have slope == 0 in their LassoResult, but their raw sxy is preserved.
        val lambda = 1e6
        val full = Lasso(lambda).apply {
            for (x in 0..19) update(x.toDouble(), 3.0 * x + 2.0)
        }
        val left = Lasso(lambda).apply {
            for (x in 0..9) update(x.toDouble(), 3.0 * x + 2.0)
        }
        val right = Lasso(lambda).apply {
            for (x in 10..19) update(x.toDouble(), 3.0 * x + 2.0)
        }
        assertEquals(0.0, left.read().slope, EPS)
        assertEquals(0.0, right.read().slope, EPS)

        left.merge(right.read())
        assertEquals(full.read().sxy, left.read().sxy, APPROX)
        assertEquals(full.read().y.mean, left.read().y.mean, APPROX)
    }

    @Test
    fun `reset clears state`() {
        val lasso = Lasso(lambda = 1.0).apply {
            for (x in 0..9) update(x.toDouble(), x.toDouble())
        }
        lasso.reset()
        val r = lasso.read()
        assertEquals(0.0, r.totalWeights, EPS)
        assertEquals(0.0, r.slope, EPS)
        assertEquals(0.0, r.sxy, EPS)
    }

    @Test
    fun `create produces fresh independent stat with same lambda`() {
        val l1 = Lasso(lambda = 0.5).apply {
            for (x in 0..4) update(x.toDouble(), x.toDouble())
        }
        val l2 = l1.create()
        l2.update(100.0, 200.0)
        assertEquals(5.0, l1.read().totalWeights, EPS)
        assertEquals(1.0, l2.read().totalWeights, EPS)
        assertEquals(0.5, l2.read().lambda, EPS)
    }
}

