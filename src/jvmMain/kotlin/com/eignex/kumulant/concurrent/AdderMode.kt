package com.eignex.kumulant.concurrent

import java.util.concurrent.atomic.DoubleAdder as JDoubleAdder
import java.util.concurrent.atomic.LongAdder as JLongAdder

object AdderMode : StreamMode {
    override fun newDouble(initial: Double) = DoubleAdder(initial)
    override fun newLong(initial: Long) = LongAdder(initial)
    override fun <T> newReference(initial: T) = AtomicReference(initial)
}

@JvmInline
value class DoubleAdder(val ref: JDoubleAdder) : StreamDouble {

    constructor(initial: Double = 0.0) : this(
        JDoubleAdder().also {
            it.add(initial)
        }
    )

    override fun load(): Double {
        return ref.sum()
    }

    override fun store(value: Double) {
        ref.reset()
        ref.add(value)
    }

    override fun add(delta: Double) {
        ref.add(delta)
    }

    override fun addAndGet(delta: Double): Double {
        ref.add(delta)
        return ref.sum()
    }

    override fun getAndAdd(delta: Double): Double {
        val ret = ref.sum()
        ref.add(delta)
        return ret
    }
}

@JvmInline
value class LongAdder(val ref: JLongAdder) : StreamLong {
    constructor(initial: Long = 0L) : this(JLongAdder().also { it.add(initial) })

    override fun load(): Long {
        return ref.sum()
    }

    override fun store(value: Long) {
        ref.reset()
        ref.add(value)
    }

    override fun add(delta: Long) {
        ref.add(delta)
    }

    override fun addAndGet(delta: Long): Long {
        ref.add(delta)
        return ref.sum()
    }

    override fun getAndAdd(delta: Long): Long {
        val ret = ref.sum()
        ref.add(delta)
        return ret
    }
}
