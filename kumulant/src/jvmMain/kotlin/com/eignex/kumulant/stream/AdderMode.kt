package com.eignex.kumulant.stream

import java.util.concurrent.atomic.DoubleAdder as JDoubleAdder
import java.util.concurrent.atomic.LongAdder as JLongAdder

/**
 * JVM [StreamMode] backed by `java.util.concurrent.atomic.DoubleAdder` / `LongAdder`.
 *
 * Optimized for many concurrent writers (striped cells). `load()` sweeps all cells and
 * is slower than CAS atomics, so prefer this for write-heavy counters read infrequently.
 * `load` is non-linearizable: use [AtomicMode] if reads must observe a single update
 * atomically with subsequent writes.
 */
internal object AdderMode : StreamMode {
    override fun newDouble(initial: Double) = DoubleAdder(initial)
    override fun newLong(initial: Long) = LongAdder(initial)
    override fun <T> newReference(initial: T): AtomicReference<T> {
        rejectBoxedPrimitive(initial)
        return AtomicReference(initial)
    }

    /**
     * Falls back to the [AtomicMode] flat-array backing. A per-slot striped adder for
     * O(N) sketches with thousands of bins would balloon memory; consistent with the
     * scalar `compareAndSet` throwing on `DoubleAdder`/`LongAdder`, the array path
     * just delegates to single-cell atomics.
     */
    override fun newLongArray(size: Int, init: (Int) -> Long): StreamLongArray = AtomicMode.newLongArray(size, init)

    override fun newDoubleArray(size: Int, init: (Int) -> Double): StreamDoubleArray =
        AtomicMode.newDoubleArray(size, init)
}

/**
 * [StreamDouble] backed by a striped `java.util.concurrent.atomic.DoubleAdder`.
 *
 * `sum` accumulates from a `+0.0` base, so this cell cannot represent a negative zero: seeding it with
 * `-0.0` reports `+0.0`. Only the sign of a reported zero is affected, and it is inherent to a striped
 * counter rather than something the operations below can preserve.
 */
@JvmInline
internal value class DoubleAdder(val ref: JDoubleAdder) : StreamDouble {

    constructor(initial: Double = 0.0) : this(
        JDoubleAdder().also {
            it.add(initial)
        },
    )

    override fun load(): Double = ref.sum()

    // One relative add rather than reset-then-add. `reset` zeroes the base and then each cell in turn
    // while `sum` walks the same table, so a concurrent reader can pick up a partial residue that was
    // never the total at any instant - and DoubleAdder.reset is documented as effective only with no
    // concurrent updates, while StreamDouble.store promises an overwrite. Expressed as a delta, every
    // observation stays a real total and a racing add is folded in rather than discarded. Two racing
    // stores still leave one of them plus drift, which is what a striped counter can offer.
    override fun store(value: Double) {
        ref.add(value - ref.sum())
    }

    override fun add(delta: Double) {
        // Same short-circuit AtomicMode takes, so a cell holding -0.0 keeps its sign under every level
        // rather than flipping to +0.0 here and staying -0.0 under Relaxed and Strict.
        if (delta == 0.0) return
        ref.add(delta)
    }

    // Add then sum, which is deliberately not linearizable: a striped counter has no atomic
    // read-modify-write, and the returned total may already include a concurrent writer's delta. Safe
    // for "add and report roughly where we are", but NOT for the `addAndGet(1) == 1` first-writer
    // election used elsewhere in the library - two threads racing it can both observe 2, so neither
    // wins and the baseline is never seeded. Every election site sits on monotonicMode or
    // firstWriterMode, both of which resolve to AtomicMode, where the operation is a single atomic.
    override fun addAndGet(delta: Double): Double {
        ref.add(delta)
        return ref.sum()
    }

    override fun compareAndSet(expectedValue: Double, newValue: Double): Boolean = throw UnsupportedOperationException(
        "DoubleAdder does not support compareAndSet; use AtomicMode for CAS-based stats",
    )
}

/** [StreamLong] backed by a striped `java.util.concurrent.atomic.LongAdder`. */
@JvmInline
internal value class LongAdder(val ref: JLongAdder) : StreamLong {
    constructor(initial: Long = 0L) : this(JLongAdder().also { it.add(initial) })

    override fun load(): Long = ref.sum()

    // One relative add rather than reset-then-add. `reset` zeroes the base and then each cell in turn
    // while `sum` walks the same table, so a concurrent reader can pick up a partial residue that was
    // never the total at any instant - and DoubleAdder.reset is documented as effective only with no
    // concurrent updates, while StreamDouble.store promises an overwrite. Expressed as a delta, every
    // observation stays a real total and a racing add is folded in rather than discarded. Two racing
    // stores still leave one of them plus drift, which is what a striped counter can offer.
    override fun store(value: Long) {
        ref.add(value - ref.sum())
    }

    override fun add(delta: Long) {
        // Same short-circuit AtomicMode takes, so a cell holding -0L keeps its sign under every level
        // rather than flipping to +0L here and staying -0L under Relaxed and Strict.
        if (delta == 0L) return
        ref.add(delta)
    }

    // Add then sum, which is deliberately not linearizable: a striped counter has no atomic
    // read-modify-write, and the returned total may already include a concurrent writer's delta. Safe
    // for "add and report roughly where we are", but NOT for the `addAndGet(1) == 1` first-writer
    // election used elsewhere in the library - two threads racing it can both observe 2, so neither
    // wins and the baseline is never seeded. Every election site sits on monotonicMode or
    // firstWriterMode, both of which resolve to AtomicMode, where the operation is a single atomic.
    override fun addAndGet(delta: Long): Long {
        ref.add(delta)
        return ref.sum()
    }

    override fun compareAndSet(expectedValue: Long, newValue: Long): Boolean = throw UnsupportedOperationException(
        "LongAdder does not support compareAndSet; use AtomicMode for CAS-based stats",
    )
}
