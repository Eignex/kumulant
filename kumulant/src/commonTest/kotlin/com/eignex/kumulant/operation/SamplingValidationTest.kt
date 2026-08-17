package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SamplingValidationTest {

    @Test
    fun `throttle rejects a period below one in every modality`() {
        // A period of zero would divide by zero in the tick test, and a negative one would forward on a
        // schedule nobody could predict. Both are caller errors rather than degenerate-but-valid
        // configurations, so they throw at construction rather than at the first update.
        for (every in listOf(0, -1, -7)) {
            assertFailsWith<IllegalArgumentException>("series accepted every=$every") {
                SumStat().throttle(every)
            }
            assertFailsWith<IllegalArgumentException>("paired accepted every=$every") {
                SumStat().atY().throttle(every)
            }
            assertFailsWith<IllegalArgumentException>("vector accepted every=$every") {
                VectorizedStat(2, SumStat()).throttle(every)
            }
            assertFailsWith<IllegalArgumentException>("regression accepted every=$every") {
                StochasticRegressionStat(featureSize = 2).throttle(every)
            }
        }
    }

    @Test
    fun `throttle accepts a period of one as forward-everything`() {
        // The boundary the check has to leave open: `every = 1` is the identity, not a rejection.
        val stat = SumStat().throttle(1)
        stat.update(3.0)
        stat.update(4.0)
        assertEquals(7.0, stat.read().sum, 1e-12)
    }

    @Test
    fun `sample rejects a rate outside the unit interval in every modality`() {
        for (rate in listOf(-0.1, 1.1, Double.NaN)) {
            assertFailsWith<IllegalArgumentException>("series accepted rate=$rate") {
                SumStat().sample(rate, Random(0))
            }
            assertFailsWith<IllegalArgumentException>("paired accepted rate=$rate") {
                SumStat().atY().sample(rate, Random(0))
            }
            assertFailsWith<IllegalArgumentException>("vector accepted rate=$rate") {
                VectorizedStat(2, SumStat()).sample(rate, Random(0))
            }
            assertFailsWith<IllegalArgumentException>("regression accepted rate=$rate") {
                StochasticRegressionStat(featureSize = 2).sample(rate, Random(0))
            }
        }
    }

    @Test
    fun `sample accepts both endpoints`() {
        // Zero and one are the drop-everything and keep-everything ends, both legitimate.
        val none = SumStat().sample(0.0, Random(0))
        repeat(20) { none.update(1.0) }
        assertEquals(0.0, none.read().sum, 1e-12, "a rate of zero should drop every update")

        val all = SumStat().sample(1.0, Random(0))
        repeat(20) { all.update(1.0) }
        assertEquals(20.0, all.read().sum, 1e-12, "a rate of one should keep every update")
    }

    @Test
    fun `the rejection says which bound was violated`() {
        val every = assertFailsWith<IllegalArgumentException> { SumStat().throttle(0) }
        assertTrue("throttle every must be >= 1" in every.message.orEmpty(), "unhelpful: ${every.message}")
        assertTrue("got 0" in every.message.orEmpty(), "did not name the value: ${every.message}")

        val rate = assertFailsWith<IllegalArgumentException> { SumStat().sample(1.5, Random(0)) }
        assertTrue("sample rate must be in [0, 1]" in rate.message.orEmpty(), "unhelpful: ${rate.message}")
        assertTrue("got 1.5" in rate.message.orEmpty(), "did not name the value: ${rate.message}")
    }
}
