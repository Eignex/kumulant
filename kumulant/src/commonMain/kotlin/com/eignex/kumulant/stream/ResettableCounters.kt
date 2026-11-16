package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.util.concurrent.CellMode
import com.eignex.util.concurrent.CounterMode
import com.eignex.util.concurrent.DoubleCounter
import com.eignex.util.concurrent.LongCounter

// Util counters are additive-only. Stats own their reset boundary by publishing a fresh counter,
// retaining striped HighWrite updates. An update overlapping reset may belong to either interval.
internal fun Concurrency.resettableMode(): ResettableCounterMode =
    ResettableCounterMode(additiveMode(), monotonicMode())

internal class ResettableCounterMode(private val counters: CounterMode, private val references: CellMode) {
    fun newDouble(initial: Double = 0.0): ResettableDoubleCounter =
        ResettableDoubleCounter(counters, references, initial)

    fun newLong(initial: Long = 0L): ResettableLongCounter =
        ResettableLongCounter(counters, references, initial)
}

internal class ResettableDoubleCounter(
    private val mode: CounterMode,
    references: CellMode,
    private val initial: Double,
) : DoubleCounter {
    private val current = references.newReference(mode.newDouble(initial))

    override fun load(): Double = current.load().load()
    override fun add(delta: Double) = current.load().add(delta)
    fun reset() = current.store(mode.newDouble(initial))
}

internal class ResettableLongCounter(
    private val mode: CounterMode,
    references: CellMode,
    private val initial: Long,
) : LongCounter {
    private val current = references.newReference(mode.newLong(initial))

    override fun load(): Long = current.load().load()
    override fun add(delta: Long) = current.load().add(delta)
    fun reset() = current.store(mode.newLong(initial))
}
