package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.AtomicMode
import com.eignex.kumulant.concurrent.SerialMode
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
        repeat(5) { ols.update(3.0, it.toDouble()) } // sxx = 0
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
        // x goes up, y is all over the place
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
    fun `copy is independent`() {
        val ols1 = OLS(AtomicMode, "orig").apply {
            for (x in 0..4) update(x.toDouble(), x.toDouble())
        }
        val ols2 = ols1.copy(SerialMode, "copy")
        ols2.update(100.0, 200.0)
        // ols1 must not be affected
        assertEquals(5.0, ols1.read().totalWeights, EPS)
        assertEquals("copy", ols2.read().name)
    }

    @Test
    fun `predict uses slope and intercept`() {
        val ols =
            OLS().apply { for (x in 0..9) update(x.toDouble(), 2.0 * x + 1.0) }
        val r = ols.read()
        assertEquals(11.0, r.predict(5.0), APPROX)
    }
}

class CovarianceTest {

    @Test
    fun `covariance of perfectly correlated data`() {
        val cov = Covariance()
        for (i in 1..5) cov.update(i.toDouble(), i.toDouble())
        val r = cov.read()
        // cov(X, X) = var(X); for [1,2,3,4,5] pop-var = 2.0
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
        // X and Y are independent by symmetry: meanX=0, meanY=0, sxy=0
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
        // y = 2x exactly → correlation = 1
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
    fun `copy produces fresh independent stat`() {
        val c1 = Covariance(name = "orig").apply {
            update(1.0, 2.0)
            update(2.0, 4.0)
        }
        val c2 = c1.copy(name = "copy")
        c2.update(5.0, 10.0)
        // c1 unchanged
        assertEquals(2.0, c1.read().totalWeights, EPS)
        assertEquals(1.0, c2.read().totalWeights, EPS)
        assertEquals("copy", c2.read().name)
    }
}

// Planned: WLSTest — tests for weighted least squares once WLS is implemented
// Planned: RidgeTest — tests for Ridge regression once implemented
