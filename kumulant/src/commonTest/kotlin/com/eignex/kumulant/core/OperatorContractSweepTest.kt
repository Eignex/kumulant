package com.eignex.kumulant.core

import com.eignex.kumulant.operation.derivative
import com.eignex.kumulant.operation.diff
import com.eignex.kumulant.operation.hysteresis
import com.eignex.kumulant.operation.lag
import com.eignex.kumulant.operation.resampleByTime
import com.eignex.kumulant.operation.sample
import com.eignex.kumulant.operation.throttle
import com.eignex.kumulant.operation.withSelfLag
import com.eignex.kumulant.stat.regression.CovarianceStat
import com.eignex.kumulant.stat.summary.SummaryStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

private const val NS_PER_SEC = 1_000_000_000L

// The sibling of SeriesStatContractSweepTest for the operator layer. That sweep materializes bare
// stats from specs, so an operator wrapping one is outside it - which is how a whole family of
// operators came to carry phase across an inert weight while every stat underneath honoured it.
//
// The operators here are exactly those holding state between updates: a ring, a phase counter, a
// debounced level. A stateless operator (filter, transform, fold, selector) cannot violate the
// contract, since it has nothing to move.
class OperatorContractSweepTest {

    private val readAt = 9L * NS_PER_SEC

    // An operator's state is only observable through what it forwards, so each entry replays the
    // same stream twice: once with an inert-weight observation spliced in, once without. The two
    // must agree, and the probe value is far outside the stream so absorbing it anywhere shows up.
    //
    // SummaryStat rather than SumStat as the delegate, because a sum telescopes under diff: the
    // extra terms an absorbed observation contributes are (probe - prev) + (next - probe), which is
    // exactly the (next - prev) the sum would have seen anyway. A delegate carrying count and
    // extrema cannot cancel that way, and the vacuity guard below is what caught it.
    private val operators: List<Pair<String, () -> SeriesStat<*>>> = listOf(
        "lag(1)" to { SummaryStat().lag(1) },
        "lag(3)" to { SummaryStat().lag(3) },
        "diff(1)" to { SummaryStat().diff(1) },
        "diff(2)" to { SummaryStat().diff(2) },
        "derivative" to { SummaryStat().derivative() },
        "withSelfLag(1)" to { CovarianceStat().withSelfLag(1) },
        "throttle(2)" to { SummaryStat().throttle(2) },
        "throttle(3)" to { SummaryStat().throttle(3) },
        "sample(0.5)" to { SummaryStat().sample(0.5, seed = 42L) },
        "hysteresis" to { SummaryStat().hysteresis(1.0, 5.0) },
        "resampleByTime" to { SummaryStat().resampleByTime(2.seconds) },
    )

    private val samples = doubleArrayOf(3.0, 7.5, 1.0, 12.0, 6.25, 9.0, 2.5, 11.0)

    private fun replay(stat: SeriesStat<*>, inertWeight: Double?, probe: Double) {
        samples.forEachIndexed { i, v ->
            if (inertWeight != null && i == 4) {
                stat.update(probe, i.toLong() * NS_PER_SEC, inertWeight)
            }
            stat.update(v, i.toLong() * NS_PER_SEC, 1.0)
        }
    }

    private fun sweep(weight: Double, label: String) {
        val violations = mutableListOf<String>()
        for ((name, build) in operators) {
            for (probe in doubleArrayOf(999.0, -999.0)) {
                val withProbe = build().also { replay(it, weight, probe) }
                val without = build().also { replay(it, null, probe) }

                val a = withProbe.read(readAt)
                val b = without.read(readAt)
                if (a != b) violations += "$name absorbed a $label $probe: $a vs $b"
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
        // Vacuity guard: the probe has to be an observation these operators visibly absorb at a live
        // weight, or "state did not change" proves nothing about the inert case.
        val inert = operators.filter { (_, build) ->
            val withProbe = build().also { replay(it, 1.0, 999.0) }
            val without = build().also { replay(it, null, 999.0) }
            withProbe.read(readAt) == without.read(readAt)
        }.map { it.first }
        assertEquals(emptyList(), inert.toList(), "every operator must absorb a live-weight probe")
    }
}
