package com.eignex.kumulant.operation

import com.eignex.koblas.F64DenseVector
import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun sumVector(d: Int) = VectorizedStat(d, SumStat())

class WeightsTest {

    @Test
    fun `series withWeight overrides caller weight`() {
        val stat = SumStat().withWeight(2.0)
        stat.update(3.0, weight = 100.0)
        assertEquals(6.0, stat.read().sum, DELTA)
    }

    @Test
    fun `series withWeight create preserves weight`() {
        val template = SumStat().withWeight(3.0)
        val fresh = template.create()
        fresh.update(2.0)
        assertEquals(6.0, fresh.read().sum, DELTA)
    }

    @Test
    fun `paired withWeight overrides caller weight`() {
        val stat = SumStat().atY().withWeight(2.0)
        stat.update(0.0, 5.0)
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `vector withWeight overrides caller weight`() {
        val stat = sumVector(2).withWeight(2.0)
        stat.update(doubleArrayOf(3.0, 4.0))
        assertEquals(6.0, stat.read().results[0].sum, DELTA)
        assertEquals(8.0, stat.read().results[1].sum, DELTA)
    }

    @Test
    fun `discrete withWeight at zero drops all updates`() {
        val stat = HyperLogLogStat(precision = 10).withWeight(0.0)
        for (i in 1L..100L) stat.update(i, weight = 1.0)
        assertEquals(0.0, stat.read().estimate)
    }

    @Test
    fun `discrete withWeight overrides a positive caller weight`() {
        val stat = HyperLogLogStat(precision = 10).withWeight(1.0)
        for (i in 1L..50L) stat.update(i, weight = 0.25)
        assertTrue(stat.read().estimate > 30.0)
    }

    @Test
    fun `withWeight passes a zero caller weight through instead of overriding it`() {
        // Overriding the zero would break the library-wide guarantee on Stat that a weight of 0.0
        // changes no state whatever the modality. The override sets the magnitude of a real
        // observation, and "ignore this observation" is not a magnitude.
        val discrete = HyperLogLogStat(precision = 10).withWeight(1.0)
        for (i in 1L..50L) discrete.update(i, weight = 0.0)
        assertEquals(0.0, discrete.read().estimate, "a zero-weight update must not register")

        val counted = CountStat()
        counted.update(5.0, weight = 0.0)
        assertEquals(0.0, counted.read().sum, "CountStat is SumStat.withWeight(1.0) and must not count either")
        counted.update(5.0, weight = 1.0)
        assertEquals(1.0, counted.read().sum, "a real observation still counts as exactly one")
    }

    @Test
    fun `withWeight passes an inert caller weight through for every modality`() {
        // Zero and NaN are both inert; see Stat. Each modality has its own adapter, so sweeping all
        // five is what stops one of them drifting from the rest.
        for (inert in listOf(0.0, Double.NaN)) {
            val series = SumStat().withWeight(2.0)
            series.update(3.0, weight = inert)
            assertEquals(0.0, series.read().sum, DELTA, "series moved on weight $inert")

            val paired = SumStat().atY().withWeight(2.0)
            paired.update(0.0, 5.0, weight = inert)
            assertEquals(0.0, paired.read().sum, DELTA, "paired moved on weight $inert")

            val vector = sumVector(2).withWeight(2.0)
            vector.update(doubleArrayOf(3.0, 4.0), weight = inert)
            assertEquals(0.0, vector.read().results[0].sum, DELTA, "vector moved on weight $inert")

            val discrete = HyperLogLogStat(precision = 10).withWeight(1.0)
            for (i in 1L..50L) discrete.update(i, weight = inert)
            assertEquals(0.0, discrete.read().estimate, "discrete moved on weight $inert")

            val regression = StochasticRegressionStat(featureSize = 2).withWeight(2.0)
            regression.update(F64DenseVector.of(doubleArrayOf(1.0, 1.0)), 1.0, weight = inert)
            assertEquals(0.0, regression.read().totalWeights, DELTA, "regression moved on weight $inert")
        }
    }
}
