package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-9

class VarianceVectorTest {

    @Test
    fun `each dimension matches an independent Variance`() {
        val rows = listOf(
            doubleArrayOf(1.0, 10.0, 100.0),
            doubleArrayOf(2.0, 20.0, 200.0),
            doubleArrayOf(3.0, 30.0, 300.0),
        )
        val vec = varianceVector(3)
        val refs = Array(3) { Variance() }
        for (row in rows) {
            vec.update(row, 0L, 1.0)
            for (d in 0 until 3) refs[d].update(row[d], 0L, 1.0)
        }
        val results = vec.read(0L).results
        assertEquals(3, results.size)
        for (d in 0 until 3) {
            val expected = refs[d].read(0L)
            assertEquals(expected.totalWeights, results[d].totalWeights, DELTA)
            assertEquals(expected.mean, results[d].mean, DELTA)
            assertEquals(expected.variance, results[d].variance, DELTA)
        }
    }
}
