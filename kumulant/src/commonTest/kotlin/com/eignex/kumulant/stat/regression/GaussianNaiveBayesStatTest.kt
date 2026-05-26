package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.math.DenseVector
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class GaussianNaiveBayesStatTest {

    @Test
    fun `per class means and variances match the running stats`() {
        val stat = GaussianNaiveBayesStat(featureSize = 2, numClasses = 2)
        // Class 0: feature 0 around 1.0, feature 1 around 5.0.
        stat.update(doubleArrayOf(1.0, 5.0), 0.0)
        stat.update(doubleArrayOf(3.0, 5.0), 0.0)
        // Class 1: feature 0 around -1.0, feature 1 around 2.0.
        stat.update(doubleArrayOf(-1.0, 2.0), 1.0)
        stat.update(doubleArrayOf(-3.0, 2.0), 1.0)
        val r = stat.read()
        assertEquals(2.0, r.means[0, 0], DELTA)
        assertEquals(5.0, r.means[0, 1], DELTA)
        assertEquals(-2.0, r.means[1, 0], DELTA)
        assertEquals(2.0, r.means[1, 1], DELTA)
        // Population variance of (1, 3) is 1.0; of (-1, -3) is 1.0; constants are 0.
        assertEquals(1.0, r.variances[0, 0], DELTA)
        assertEquals(0.0, r.variances[0, 1], DELTA)
        assertEquals(1.0, r.variances[1, 0], DELTA)
        assertEquals(0.0, r.variances[1, 1], DELTA)
        assertEquals(0.5, r.prior(0), DELTA)
        assertEquals(0.5, r.prior(1), DELTA)
        assertEquals(4.0, r.totalWeights, DELTA)
    }

    @Test
    fun `learns a separable three-class Gaussian mixture`() {
        val stat = GaussianNaiveBayesStat(featureSize = 2, numClasses = 3)
        val rng = Random(7L)
        val centers = arrayOf(
            doubleArrayOf(3.0, 0.0),
            doubleArrayOf(-3.0, 3.0),
            doubleArrayOf(-3.0, -3.0),
        )
        repeat(900) {
            val c = rng.nextInt(3)
            val x = doubleArrayOf(
                centers[c][0] + rng.nextDouble() - 0.5,
                centers[c][1] + rng.nextDouble() - 0.5,
            )
            stat.update(x, c.toDouble())
        }
        val r = stat.read()
        var correct = 0
        val n = 300
        repeat(n) {
            val c = rng.nextInt(3)
            val x = DenseVector.of(
                doubleArrayOf(
                    centers[c][0] + rng.nextDouble() - 0.5,
                    centers[c][1] + rng.nextDouble() - 0.5,
                ),
            )
            if (r.predict(x) == c) correct++
        }
        assertTrue(correct.toDouble() / n > 0.95, "accuracy=${correct.toDouble() / n}")
    }

    @Test
    fun `probabilities sum to one and softmax matches argmax`() {
        val stat = GaussianNaiveBayesStat(featureSize = 1, numClasses = 2)
        repeat(10) { stat.update(doubleArrayOf(0.0), 0.0) }
        repeat(10) { stat.update(doubleArrayOf(10.0), 1.0) }
        val r = stat.read()
        val p = r.probabilities(DenseVector.of(doubleArrayOf(9.5)))
        assertEquals(1.0, p.sum(), 1e-9)
        assertTrue(p[1] > p[0])
        assertEquals(1, r.predict(DenseVector.of(doubleArrayOf(9.5))))
    }

    @Test
    fun `merge equals a single-stream fit for a fixed observation set`() {
        val truth = GaussianNaiveBayesStat(featureSize = 2, numClasses = 2)
        val a = GaussianNaiveBayesStat(featureSize = 2, numClasses = 2)
        val b = GaussianNaiveBayesStat(featureSize = 2, numClasses = 2)
        val rng = Random(99L)
        val xs = List(40) {
            val c = rng.nextInt(2)
            Triple(
                doubleArrayOf(rng.nextDouble() + c * 5.0, rng.nextDouble() - c * 3.0),
                c.toDouble(),
                1.0,
            )
        }
        xs.forEach { (x, y, w) -> truth.update(x, y, w) }
        xs.subList(0, 20).forEach { (x, y, w) -> a.update(x, y, w) }
        xs.subList(20, 40).forEach { (x, y, w) -> b.update(x, y, w) }
        a.merge(b.read())
        val ra = a.read()
        val rt = truth.read()
        for (c in 0..1) {
            for (i in 0..1) {
                assertTrue(abs(ra.means[c, i] - rt.means[c, i]) < 1e-9, "mean mismatch [$c,$i]")
                assertTrue(abs(ra.variances[c, i] - rt.variances[c, i]) < 1e-9, "var mismatch [$c,$i]")
            }
        }
        assertEquals(rt.totalWeights, ra.totalWeights, 1e-9)
    }

    @Test
    fun `reset zeroes all state`() {
        val stat = GaussianNaiveBayesStat(featureSize = 1, numClasses = 2)
        stat.update(doubleArrayOf(1.0), 0.0)
        stat.update(doubleArrayOf(5.0), 1.0)
        stat.reset()
        val r = stat.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.means[0, 0], DELTA)
        assertEquals(0.0, r.means[1, 0], DELTA)
    }
}
