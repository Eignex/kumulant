package com.eignex.kumulant

import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import java.util.function.IntConsumer
import kotlin.test.fail

// kotlinx-benchmark 0.4.13 does not expose JMH's `gc` profiler - its `advanced` options are limited to
// fork and bridge settings - so per-operation allocation is invisible to the benchmark suite. It is
// measured here through the HotSpot thread allocation counter instead, which is exact for the current
// thread and cheap enough to run as an ordinary test.
//
// What the counter cannot see past is the JIT. Until C2 has compiled a call chain, objects that escape
// analysis later removes are still allocated - a koblas kernel's shape record, a capturing lambda - and
// they show up as tens of bytes per call that the steady state never spends. Both helpers below are
// shaped around that: one waits for the steady state, the other measures its arms side by side so a
// transient they share cancels out.
//
// A body is an `IntConsumer` handed the call index rather than a Kotlin `(Int) -> Unit`, which compiles
// to `Function1<Integer, Unit>` and boxes the index on every call: 16 B per iteration that would swamp
// what is being measured. `IntConsumer.accept` compiles to `(I)V` and allocates nothing.

private val threads = ManagementFactory.getThreadMXBean() as ThreadMXBean

internal const val ALLOCATION_PASS_CALLS = 2_000
private const val WARMUP_CALLS = 10_000
private const val PASSES = 10
private const val SETTLE_BUDGET_NANOS = 3_000_000_000L

/** Bytes allocated per call by one pass of [body] on the current thread. */
private fun measurePass(callsPerPass: Int, body: IntConsumer): Double {
    val id = Thread.currentThread().threadId()
    val before = threads.getThreadAllocatedBytes(id)
    for (i in 0 until callsPerPass) body.accept(i)
    val after = threads.getThreadAllocatedBytes(id)
    return (after - before).toDouble() / callsPerPass
}

/**
 * Asserts that [body] settles at or under [ceiling] bytes per call.
 *
 * Passes repeat until one comes in under the ceiling, since bytes per call only fall as compilation
 * lands, and give up on a wall-clock budget rather than a call count so a slow compile queue is waited
 * out instead of raced. A pass that clears the ceiling is sound on its own: nothing the JIT does later
 * makes the same code allocate more. [callsPerPass] is raised for a body whose allocation is amortised
 * over many calls, so that one pass spans several of its epochs.
 */
internal fun assertAllocatesAtMost(
    ceiling: Double,
    what: String,
    callsPerPass: Int = ALLOCATION_PASS_CALLS,
    body: IntConsumer,
) {
    val deadline = System.nanoTime() + SETTLE_BUDGET_NANOS
    var best = Double.MAX_VALUE
    do {
        best = minOf(best, measurePass(callsPerPass, body))
        if (best <= ceiling) return
    } while (System.nanoTime() < deadline)
    fail("$what allocated $best B/call, expected at most $ceiling")
}

/**
 * Best-of-passes bytes per call for each of [bodies], measured in interleaved passes after a shared
 * warmup.
 *
 * Interleaving keeps every arm in the same JIT state as its neighbours, so a transient the arms share
 * cancels out of a difference between them; measuring one arm to completion before starting the next
 * would charge it to whichever arm ran first. Read the result differentially, asserting that one arm
 * undercuts another by the buffer it is meant to save. An absolute claim about a single arm belongs
 * in [assertAllocatesAtMost].
 */
internal fun bytesPerCall(vararg bodies: IntConsumer): DoubleArray {
    for (body in bodies) for (i in 0 until WARMUP_CALLS) body.accept(i)
    val best = DoubleArray(bodies.size) { Double.MAX_VALUE }
    repeat(PASSES) {
        for (b in bodies.indices) best[b] = minOf(best[b], measurePass(ALLOCATION_PASS_CALLS, bodies[b]))
    }
    return best
}
