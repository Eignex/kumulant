package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64StridedVectorView
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.DELTA
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class GaussianNaiveBayesStatTest {

    private class CustomVector(private val values: DoubleArray) : F64VectorLike {
        override val size: Int get() = values.size
        override fun get(i: Int): Double = values[i]
        override fun toDoubleArray(): DoubleArray = values.copyOf()
    }

    @Test
    fun `read owns matrix and vector storage independently from the stat`() {
        val stat = GaussianNaiveBayesStat(featureSize = 1, numClasses = 2)
        stat.update(doubleArrayOf(1.0), 0.0)
        val first = stat.read()
        val firstMean = first.means[0, 0]
        val firstVariance = first.variances[0, 0]
        val firstWeight = first.classWeights[0]

        stat.update(doubleArrayOf(2.0), 0.0)
        GaussianNaiveBayesStat(featureSize = 1, numClasses = 2).also {
            it.update(doubleArrayOf(3.0), 0.0)
            stat.merge(it.read())
        }
        stat.reset()
        val second = stat.read()

        assertEquals(firstMean, first.means[0, 0], DELTA)
        assertEquals(firstVariance, first.variances[0, 0], DELTA)
        assertEquals(firstWeight, first.classWeights[0], DELTA)
        first.means.data[0] = 99.0
        first.variances.data[0] = 99.0
        first.classWeights.data[0] = 99.0
        assertTrue(second.means[0, 0] != 99.0)
        assertTrue(second.variances[0, 0] != 99.0)
        assertTrue(second.classWeights[0] != 99.0)
    }

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
            val x = F64DenseVector.of(
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
        val p = r.probabilities(F64DenseVector.of(doubleArrayOf(9.5)))
        assertEquals(1.0, p.sum(), 1e-9)
        assertTrue(p[1] > p[0])
        assertEquals(1, r.predict(F64DenseVector.of(doubleArrayOf(9.5))))
    }

    @Test
    fun `destination scores match owned scores for dense sparse strided and custom inputs`() {
        val stat = GaussianNaiveBayesStat(featureSize = 3, numClasses = 2)
        stat.update(doubleArrayOf(1.0, 0.0, -2.0), 0.0, weight = 2.0)
        stat.update(doubleArrayOf(-1.0, 0.0, 3.0), 1.0)
        val result = stat.read()
        val dense = F64DenseVector.of(doubleArrayOf(0.5, 0.0, -1.0))
        val sparse = F64SparseVector.of(3, intArrayOf(0, 1, 2), doubleArrayOf(0.5, 0.0, -1.0))
        val strided = F64StridedVectorView(doubleArrayOf(0.5, 9.0, 0.0, 9.0, -1.0, 9.0), 0, 3, 2)
        val custom = CustomVector(doubleArrayOf(0.5, 0.0, -1.0))
        val expected = result.probabilities(dense)

        for (x in listOf<F64VectorLike>(sparse, strided, custom)) {
            val logs = DoubleArray(2)
            val probabilities = DoubleArray(2)
            result.logPosteriorsInto(x, logs)
            result.probabilitiesInto(x, probabilities)
            assertEquals(result.logPosterior(x, 0), logs[0], 1e-12)
            assertEquals(result.logPosterior(x, 1), logs[1], 1e-12)
            assertEquals(expected[0], probabilities[0], 1e-12)
            assertEquals(expected[1], probabilities[1], 1e-12)
            assertEquals(result.predict(dense), result.predict(x))
        }
    }

    @Test
    fun `destination validation fails before mutation`() {
        val stat = GaussianNaiveBayesStat(featureSize = 2, numClasses = 2)
        val result = stat.read()
        val destination = doubleArrayOf(7.0)

        assertFailsWith<IllegalArgumentException> {
            result.logPosteriorsInto(
                F64DenseVector.of(doubleArrayOf(0.0, 0.0)),
                destination,
            )
        }

        assertTrue(destination.contentEquals(doubleArrayOf(7.0)))
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
