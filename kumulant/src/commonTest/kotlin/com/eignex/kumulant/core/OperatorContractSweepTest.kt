package com.eignex.kumulant.core

import com.eignex.kumulant.operation.VectorizedStat
import com.eignex.kumulant.operation.asDiscrete
import com.eignex.kumulant.operation.derivative
import com.eignex.kumulant.operation.diff
import com.eignex.kumulant.operation.hysteresis
import com.eignex.kumulant.operation.lag
import com.eignex.kumulant.operation.resampleByTime
import com.eignex.kumulant.operation.sample
import com.eignex.kumulant.operation.throttle
import com.eignex.kumulant.operation.windowed
import com.eignex.kumulant.operation.withSelfLag
import com.eignex.kumulant.stat.regression.CovarianceStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.summary.SummaryStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

private const val NS_PER_SEC = 1_000_000_000L

class OperatorContractSweepTest {

    private val readAt = 9L * NS_PER_SEC
    private val samples = doubleArrayOf(3.0, 7.5, 1.0, 12.0, 6.25, 9.0, 2.5, 11.0)

    private class Case(val name: String, val read: (Double?, Double) -> Any?)

    private inline fun replay(inertWeight: Double?, probe: Double, feed: (Double, Long, Double) -> Unit) {
        samples.forEachIndexed { i, v ->
            if (inertWeight != null && i == PROBE_AT) feed(probe, i.toLong() * NS_PER_SEC, inertWeight)
            feed(v, i.toLong() * NS_PER_SEC, 1.0)
        }
    }

    // A SumStat delegate telescopes under diff and can hide an absorbed probe.
    // Use count and extrema so an extra observation remains observable.
    private fun series(name: String, build: () -> SeriesStat<*>) = Case(name) { w, probe ->
        val s = build()
        replay(w, probe) { v, ts, wt -> s.update(v, ts, wt) }
        s.read(readAt)
    }

    private fun paired(name: String, build: () -> PairedStat<*>) = Case(name) { w, probe ->
        val s = build()
        replay(w, probe) { v, ts, wt -> s.update(v, v * 0.5, ts, wt) }
        s.read(readAt)
    }

    private fun vector(name: String, build: () -> VectorStat<*>) = Case(name) { w, probe ->
        val s = build()
        replay(w, probe) { v, ts, wt -> s.update(doubleArrayOf(v, v * 0.5), ts, wt) }
        s.read(readAt)
    }

    private fun discrete(name: String, build: () -> DiscreteStat<*>) = Case(name) { w, probe ->
        val s = build()
        replay(w, probe) { v, ts, wt -> s.update(v.toLong(), ts, wt) }
        s.read(readAt)
    }

    private fun regression(name: String, build: () -> RegressionStat<*>) = Case(name) { w, probe ->
        val s = build()
        replay(w, probe) { v, ts, wt -> s.update(doubleArrayOf(v, 1.0), v * 2.0, ts, wt) }
        s.read(readAt)
    }

    private val cases: List<Case> = listOf(
        series("lag(1)") { SummaryStat().lag(1) },
        series("lag(3)") { SummaryStat().lag(3) },
        series("diff(1)") { SummaryStat().diff(1) },
        series("diff(2)") { SummaryStat().diff(2) },
        series("derivative") { SummaryStat().derivative() },
        series("hysteresis") { SummaryStat().hysteresis(1.0, 5.0) },
        series("throttle(2)") { SummaryStat().throttle(2) },
        series("throttle(3)") { SummaryStat().throttle(3) },
        series("sample(0.5)") { SummaryStat().sample(0.5, seed = 42L) },
        series("resampleByTime") { SummaryStat().resampleByTime(2.seconds) },
        series("windowed") { SummaryStat().windowed(WINDOW, SLICES) },
        series("withSelfLag(1)") { CovarianceStat().withSelfLag(1) },

        paired("paired throttle(2)") { CovarianceStat().throttle(2) },
        paired("paired sample(0.5)") { CovarianceStat().sample(0.5, seed = 42L) },
        paired("paired windowed") { CovarianceStat().windowed(WINDOW, SLICES) },

        vector("vector throttle(2)") { VectorizedStat(2, SummaryStat()).throttle(2) },
        vector("vector sample(0.5)") { VectorizedStat(2, SummaryStat()).sample(0.5, seed = 42L) },
        vector("vector windowed") { VectorizedStat(2, SummaryStat()).windowed(WINDOW, SLICES) },

        discrete("discrete throttle(2)") { SummaryStat().asDiscrete().throttle(2) },
        discrete("discrete sample(0.5)") { SummaryStat().asDiscrete().sample(0.5, seed = 42L) },
        discrete("discrete windowed") { SummaryStat().asDiscrete().windowed(WINDOW, SLICES) },

        regression("regression throttle(2)") { StochasticRegressionStat(featureSize = 2).throttle(2) },
        regression("regression sample(0.5)") { StochasticRegressionStat(featureSize = 2).sample(0.5, seed = 42L) },
    )

    private fun sweep(weight: Double, label: String) {
        val violations = mutableListOf<String>()
        for (case in cases) {
            for (probe in doubleArrayOf(999.0, -999.0)) {
                val withProbe = case.read(weight, probe)
                val without = case.read(null, probe)
                if (withProbe != without) violations += "${case.name} absorbed a $label $probe"
            }
        }
        assertEquals(emptyList(), violations.toList(), "$label updates must not move an operator")
    }

    @Test
    fun `a zero weight is a no-op for every stateful operator`() {
        sweep(0.0, "zero-weight")
    }

    @Test
    fun `a NaN weight is a no-op for every stateful operator`() {
        sweep(Double.NaN, "NaN-weighted")
    }

    @Test
    fun `an infinite weight is a no-op for every stateful operator`() {
        sweep(Double.POSITIVE_INFINITY, "+Infinity-weighted")
        sweep(Double.NEGATIVE_INFINITY, "-Infinity-weighted")
    }

    @Test
    fun `the sweep would notice an operator that absorbed the probe`() {
        val inert = cases.filter { it.read(1.0, 999.0) == it.read(null, 999.0) }.map { it.name }
        assertEquals(emptyList(), inert.toList(), "every operator must absorb a live-weight probe")
    }

    private companion object {
        const val PROBE_AT = 4

        // Keep every probe inside the window until readAt so expiry cannot hide an absorbed update.
        val WINDOW = 12.seconds
        const val SLICES = 4
    }
}
