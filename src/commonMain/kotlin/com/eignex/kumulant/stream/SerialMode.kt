package com.eignex.kumulant.stream

/** Non-atomic, single-threaded mode; cheapest path when no concurrency is required. */
object SerialMode : StreamMode {
    override fun newDouble(initial: Double) = SerialDouble(initial)
    override fun newLong(initial: Long) = SerialLong(initial)
    override fun <T> newReference(initial: T): SerialRef<T> {
        rejectBoxedPrimitive(initial)
        return SerialRef(initial)
    }

    override fun newLongArray(size: Int, init: (Int) -> Long) =
        SerialLongArray(LongArray(size, init))

    override fun newDoubleArray(size: Int, init: (Int) -> Double) =
        SerialDoubleArray(DoubleArray(size, init))
}

/** Plain-`var` [StreamLong] implementation used by [SerialMode]. */
class SerialLong(var ref: Long) : StreamLong {
    override fun load(): Long = ref

    override fun store(value: Long) {
        ref = value
    }

    override fun add(delta: Long) {
        ref += delta
    }

    override fun addAndGet(delta: Long): Long {
        ref += delta
        return ref
    }

    override fun getAndAdd(delta: Long): Long {
        val ret = ref
        ref += delta
        return ret
    }

    override fun compareAndSet(expectedValue: Long, newValue: Long): Boolean {
        if (ref == expectedValue) {
            ref = newValue
            return true
        }
        return false
    }
}

/** Plain-`var` [StreamDouble] implementation used by [SerialMode]. */
class SerialDouble(var ref: Double) : StreamDouble {
    override fun load(): Double = ref

    override fun store(value: Double) {
        ref = value
    }

    override fun add(delta: Double) {
        ref += delta
    }

    override fun addAndGet(delta: Double): Double {
        ref += delta
        return ref
    }

    override fun getAndAdd(delta: Double): Double {
        val ret = ref
        ref += delta
        return ret
    }

    override fun compareAndSet(expectedValue: Double, newValue: Double): Boolean {
        if (ref.toRawBits() == expectedValue.toRawBits()) {
            ref = newValue
            return true
        }
        return false
    }
}

/** Plain-array [StreamLongArray] implementation used by [SerialMode]. */
class SerialLongArray(val ref: LongArray) : StreamLongArray {
    override val size: Int get() = ref.size
    override fun load(index: Int): Long = ref[index]
    override fun store(index: Int, value: Long) { ref[index] = value }
    override fun add(index: Int, delta: Long) { ref[index] += delta }
    override fun addAndGet(index: Int, delta: Long): Long {
        ref[index] += delta
        return ref[index]
    }
    override fun getAndAdd(index: Int, delta: Long): Long {
        val ret = ref[index]
        ref[index] += delta
        return ret
    }
    override fun compareAndSet(index: Int, expectedValue: Long, newValue: Long): Boolean {
        if (ref[index] == expectedValue) {
            ref[index] = newValue
            return true
        }
        return false
    }
}

/** Plain-array [StreamDoubleArray] implementation used by [SerialMode]. */
class SerialDoubleArray(val ref: DoubleArray) : StreamDoubleArray {
    override val size: Int get() = ref.size
    override fun load(index: Int): Double = ref[index]
    override fun store(index: Int, value: Double) { ref[index] = value }
    override fun add(index: Int, delta: Double) { ref[index] += delta }
    override fun addAndGet(index: Int, delta: Double): Double {
        ref[index] += delta
        return ref[index]
    }
    override fun getAndAdd(index: Int, delta: Double): Double {
        val ret = ref[index]
        ref[index] += delta
        return ret
    }
    override fun compareAndSet(index: Int, expectedValue: Double, newValue: Double): Boolean {
        if (ref[index].toRawBits() == expectedValue.toRawBits()) {
            ref[index] = newValue
            return true
        }
        return false
    }
}

/** Plain-`var` [StreamRef] implementation used by [SerialMode]. */
class SerialRef<T>(var ref: T) : StreamRef<T> {
    override fun load(): T = ref

    override fun store(value: T) {
        ref = value
    }

    override fun compareAndExchange(expectedValue: T, newValue: T): T {
        if (ref === expectedValue) {
            ref = newValue
            return expectedValue
        } else {
            return ref
        }
    }

    override fun compareAndSet(expectedValue: T, newValue: T): Boolean {
        if (ref === expectedValue) {
            ref = newValue
            return true
        } else {
            return false
        }
    }
}
