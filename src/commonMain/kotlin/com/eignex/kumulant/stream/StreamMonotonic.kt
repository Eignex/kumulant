package com.eignex.kumulant.stream

/** CAS-loop monotonic max: store [candidate] iff strictly greater than current. */
fun casMax(cell: StreamLong, candidate: Long) {
    while (true) {
        val current = cell.load()
        if (candidate <= current) return
        if (cell.compareAndSet(current, candidate)) return
    }
}

/** CAS-loop monotonic max: store [candidate] iff strictly greater than current. NaN is ignored. */
fun casMax(cell: StreamDouble, candidate: Double) {
    if (candidate.isNaN()) return
    while (true) {
        val current = cell.load()
        if (candidate <= current) return
        if (cell.compareAndSet(current, candidate)) return
    }
}

/** CAS-loop monotonic min: store [candidate] iff strictly less than current. NaN is ignored. */
fun casMin(cell: StreamDouble, candidate: Double) {
    if (candidate.isNaN()) return
    while (true) {
        val current = cell.load()
        if (candidate >= current) return
        if (cell.compareAndSet(current, candidate)) return
    }
}

/** CAS-loop bitwise-OR: set every bit in [bitMask] on [cell]. No-op if all bits already set. */
fun casOr(cell: StreamLong, bitMask: Long) {
    while (true) {
        val current = cell.load()
        val updated = current or bitMask
        if (current == updated) return
        if (cell.compareAndSet(current, updated)) return
    }
}

/** CAS-loop monotonic max on slot [index] of [arr]: store [candidate] iff strictly greater. */
fun casMax(arr: StreamLongArray, index: Int, candidate: Long) {
    while (true) {
        val current = arr.load(index)
        if (candidate <= current) return
        if (arr.compareAndSet(index, current, candidate)) return
    }
}

/** CAS-loop monotonic min on slot [index] of [arr]: store [candidate] iff strictly less. */
fun casMin(arr: StreamLongArray, index: Int, candidate: Long) {
    while (true) {
        val current = arr.load(index)
        if (candidate >= current) return
        if (arr.compareAndSet(index, current, candidate)) return
    }
}

/** CAS-loop bitwise-OR on slot [index] of [arr]. No-op if all bits already set. */
fun casOr(arr: StreamLongArray, index: Int, bitMask: Long) {
    while (true) {
        val current = arr.load(index)
        val updated = current or bitMask
        if (current == updated) return
        if (arr.compareAndSet(index, current, updated)) return
    }
}
