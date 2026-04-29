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
