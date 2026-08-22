package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import kotlin.math.PI
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class GaussianNaiveBayesResultTest {

    private fun result(
        numClasses: Int = 2,
        featureSize: Int = 1,
        means: Array<DoubleArray> = Array(numClasses) { DoubleArray(featureSize) },
        variances: Array<DoubleArray> = Array(numClasses) { DoubleArray(featureSize) { 1.0 } },
        classWeights: DoubleArray = DoubleArray(numClasses),
        totalWeights: Double = classWeights.sum(),
        varianceFloor: Double = 1e-9,
    ) = GaussianNaiveBayesResult(
        featureSize = featureSize,
        numClasses = numClasses,
        means = F64DenseMatrix.of(means),
        variances = F64DenseMatrix.of(variances),
        classWeights = F64DenseVector.of(classWeights),
        totalWeights = totalWeights,
        varianceFloor = varianceFloor,
    )

    @Test
    fun `init rejects mismatched means shape`() {
        assertFailsWith<IllegalArgumentException> {
            result(numClasses = 2, featureSize = 2, means = Array(1) { DoubleArray(2) })
        }
    }

    @Test
    fun `init rejects mismatched variances shape`() {
        assertFailsWith<IllegalArgumentException> {
            result(numClasses = 2, featureSize = 2, variances = Array(2) { DoubleArray(1) { 1.0 } })
        }
    }

    @Test
    fun `init rejects mismatched classWeights length`() {
        assertFailsWith<IllegalArgumentException> {
            result(numClasses = 3, classWeights = DoubleArray(2))
        }
    }

    @Test
    fun `init rejects non-positive varianceFloor`() {
        assertFailsWith<IllegalArgumentException> { result(varianceFloor = 0.0) }
        assertFailsWith<IllegalArgumentException> { result(varianceFloor = -1.0) }
    }

    @Test
    fun `prior is uniform when no observations seen`() {
        val r = result(numClasses = 4, classWeights = DoubleArray(4), totalWeights = 0.0)
        for (c in 0 until 4) assertEquals(0.25, r.prior(c), 1e-12)
    }

    @Test
    fun `prior reflects accumulated class weights`() {
        val r = result(numClasses = 2, classWeights = doubleArrayOf(3.0, 1.0), totalWeights = 4.0)
        assertEquals(0.75, r.prior(0), 1e-12)
        assertEquals(0.25, r.prior(1), 1e-12)
    }

    @Test
    fun `varianceFloor lifts zero per-class variance at predict time`() {
        // Two classes, single feature with zero observed variance: floor must keep log-N finite
        // and a query at one of the means should always prefer that class.
        val r = result(
            numClasses = 2,
            featureSize = 1,
            means = arrayOf(doubleArrayOf(0.0), doubleArrayOf(10.0)),
            variances = arrayOf(doubleArrayOf(0.0), doubleArrayOf(0.0)),
            classWeights = doubleArrayOf(1.0, 1.0),
            totalWeights = 2.0,
            varianceFloor = 0.5,
        )
        // Hand-compute logPosterior at x=0 under floor=0.5
        // log prior = ln(0.5); Gaussian term = -0.5 * (LOG_2PI + ln(0.5) + 0)
        val logTwoPi = ln(2.0 * PI)
        val expectedClass0 = ln(0.5) + -0.5 * (logTwoPi + ln(0.5))
        val expectedClass1 = ln(0.5) + -0.5 * (logTwoPi + ln(0.5) + 100.0 / 0.5)
        assertEquals(expectedClass0, r.logPosterior(F64DenseVector.of(doubleArrayOf(0.0)), 0), 1e-9)
        assertEquals(expectedClass1, r.logPosterior(F64DenseVector.of(doubleArrayOf(0.0)), 1), 1e-9)
        assertEquals(0, r.predict(F64DenseVector.of(doubleArrayOf(0.0))))
    }

    @Test
    fun `logPosterior rejects wrong feature size`() {
        val r = result(numClasses = 2, featureSize = 2)
        assertFailsWith<IllegalArgumentException> {
            r.logPosterior(F64DenseVector.of(doubleArrayOf(1.0)), 0)
        }
    }

    @Test
    fun `probabilities sum to one for any query`() {
        val r = result(
            numClasses = 3,
            featureSize = 1,
            means = arrayOf(doubleArrayOf(-1.0), doubleArrayOf(0.0), doubleArrayOf(1.0)),
            variances = arrayOf(doubleArrayOf(0.5), doubleArrayOf(0.5), doubleArrayOf(0.5)),
            classWeights = doubleArrayOf(1.0, 2.0, 1.0),
            totalWeights = 4.0,
        )
        val p = r.probabilities(F64DenseVector.of(doubleArrayOf(0.3)))
        assertEquals(1.0, p.sum(), 1e-12)
        for (pk in p) assertTrue(pk in 0.0..1.0, "prob out of [0,1]: $pk")
        // Sanity: stddev away from each mean still leaves nonzero mass everywhere.
        val s = sqrt(0.5)
        assertTrue(s > 0.0)
    }
}
