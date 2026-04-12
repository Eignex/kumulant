import com.eignex.kumulant.core.WeightedMeanResult
import com.eignex.kumulant.core.WeightedVarianceResult
import com.eignex.kumulant.stat.EwmaMean
import com.eignex.kumulant.stat.EwmaVariance
import kotlin.test.*

class EwmaMeanTest {
    private val delta = 1e-9

    @Test
    fun `EwmaMean create produces fresh independent stat`() {
        val m1 = EwmaMean(alpha = 0.5).apply { update(10.0) }
        val m2 = m1.create() // creates a fresh empty instance
        repeat(10) { m1.update(10.0) }
        assertEquals(0.0, m2.read().mean, delta)
        assertTrue(m1.read().mean > 0.0)
    }

    @Test
    fun `EwmaMean merge behavior`() {
        val d1 = EwmaMean(alpha = 0.5)
        d1.update(10.0)
        val d2 = WeightedMeanResult(1.0, 20.0)

        d1.merge(d2)
        // (10 + 20) / 2 = 15
        assertEquals(15.0, d1.read().mean, delta)
    }

    @Test
    fun `EwmaMean biases toward heavy recent values`() {
        val stat = EwmaMean(alpha = 0.5)
        stat.update(10.0, 1.0)
        stat.update(100.0, 10.0) // Massive recent update

        // A simple mean would be ~91.8, but a heavily decayed mean
        // will aggressively track the latest dense value.
        assertTrue(
            stat.read().mean > 80.0,
            "Mean should heavily favor the massive recent update"
        )
    }

    @Test
    fun `EwmaMean reset clears state`() {
        val meanStat = EwmaMean(alpha = 0.5)
        meanStat.update(10.0)
        meanStat.reset()
        assertEquals(0.0, meanStat.read().mean, delta)
    }
}

class EwmaVarianceTest {
    private val delta = 1e-9

    @Test
    fun `EwmaVariance create produces fresh independent stat`() {
        val v1 = EwmaVariance(alpha = 0.5).apply {
            update(1.0)
            update(2.0)
        }
        val v2 = v1.create() // creates a fresh empty instance
        repeat(10) { v1.update(1000.0) }
        assertEquals(0.0, v2.read().totalWeights, delta)
        assertTrue(v1.read().totalWeights > 0.0)
    }

    @Test
    fun `EwmaVariance tracking volatility shift`() {
        val stat = EwmaVariance(alpha = 0.1)

        // Phase 1: Low variance
        repeat(50) { stat.update(10.0, 1.0) }
        val lowVar = stat.read().variance

        // Phase 2: Massive spike
        stat.update(1000.0, 1.0)
        val highVar = stat.read().variance

        assertTrue(highVar > lowVar, "Variance should spike on outlier")
    }

    @Test
    fun `EwmaVariance empty merge`() {
        val stat = EwmaVariance(alpha = 0.1)
        stat.update(10.0, 1.0)
        stat.update(20.0, 1.0)
        val currentVar = stat.read().variance

        // Merge with zero-weight remote
        stat.merge(WeightedVarianceResult(0.0, 0.0, 0.0))

        assertEquals(currentVar, stat.read().variance, delta)
    }

    @Test
    fun `EwmaVariance bias correction prevents zero division`() {
        val stat = EwmaVariance(alpha = 0.1)
        // Should return 0.0 or something sensible, not NaN, before updates
        assertEquals(0.0, stat.read().mean, delta)
        assertEquals(0.0, stat.read().variance, delta)
    }

    @Test
    fun `EwmaVariance reset clears state`() {
        val varStat = EwmaVariance(alpha = 0.5)
        varStat.update(10.0)
        varStat.update(20.0)
        varStat.reset()
        assertEquals(0.0, varStat.read().mean, delta)
        assertEquals(0.0, varStat.read().variance, delta)
        assertEquals(0.0, varStat.read().totalWeights, delta)
    }
}
