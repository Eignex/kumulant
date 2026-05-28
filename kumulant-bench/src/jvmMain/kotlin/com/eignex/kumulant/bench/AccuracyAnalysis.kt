package com.eignex.kumulant.bench

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import kotlin.math.abs

/**
 * Serial accuracy report: for every [StatSpec], runs the deterministic workload
 * under [Concurrency.None] and prints the snapshot scalar alongside the analytical
 * reference and the absolute/relative error.
 *
 * This is the canonical "how close is the online estimator to the closed-form
 * answer" sweep; useful for inspecting the bias-vs-cost tradeoff on sketches
 * (HyperLogLog standard error, t-digest centroid error, etc.) and for spotting
 * regressions in deterministic stats (Sum, Mean, Variance).
 *
 * Invoke via `./gradlew :kumulant-bench:analyzeAccuracy`.
 */
fun main() {
    val n = 5_000
    val seed = 0xC0FFEE.toInt()

    println("Accuracy report; $n updates, seed=0x${seed.toString(16)}, Concurrency.None")
    println(
        "%-32s  %18s  %18s  %14s  %14s".format(
            "stat", "snapshot", "reference", "abs error", "rel error",
        ),
    )
    println("-".repeat(110))

    for (spec in allSpecs) {
        measure(spec, n, seed)
    }
}

private fun <S, R : Result> measure(spec: StatSpec<S, R>, n: Int, seed: Int) {
    val got = spec.runSerial(seed, n, Concurrency.None)
    val want = spec.expected(seed, n)
    val absErr = abs(got - want)
    val relErr = if (want != 0.0) absErr / abs(want) else absErr
    println(
        "%-32s  %18.6g  %18.6g  %14.4g  %14.4g".format(
            spec.name, got, want, absErr, relErr,
        ),
    )
}
