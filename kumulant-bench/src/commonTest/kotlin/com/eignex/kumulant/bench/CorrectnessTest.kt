package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Replay each [StatSpec]'s deterministic workload serially under [Concurrency.None]
 * and assert the snapshot scalar matches the spec's analytical reference within
 * its tolerance. Catches arithmetic regressions without exercising threading.
 */
class CorrectnessTest {

    @Test
    fun `every spec matches its reference under None`() {
        for (spec in allSpecs) checkSpec(spec)
    }

    private fun <S, R : Result> checkSpec(spec: StatSpec<S, R>) {
        val n = 5_000
        val seed = 0xC0FFEE.toInt()
        val got = spec.runSerial(seed, n, Concurrency.None)
        val want = spec.expected(seed, n)
        assertTrue(
            abs(got - want) <= spec.tolerance,
            "${spec.name}: got=$got want=$want diff=${abs(got - want)} tol=${spec.tolerance}",
        )
    }
}
