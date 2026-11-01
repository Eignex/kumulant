package com.eignex.kumulant.stream

internal fun casMax(cell: StreamLong, candidate: Long) {
    while (true) {
        val current = cell.load()
        if (candidate <= current) return
        if (cell.compareAndSet(current, candidate)) return
    }
}

internal fun casMin(cell: StreamLong, candidate: Long) {
    while (true) {
        val current = cell.load()
        if (candidate >= current) return
        if (cell.compareAndSet(current, candidate)) return
    }
}

internal fun casMax(cell: StreamDouble, candidate: Double) {
    if (candidate.isNaN()) return
    while (true) {
        val current = cell.load()
        if (candidate <= current) return
        if (cell.compareAndSet(current, candidate)) return
    }
}

internal fun casMin(cell: StreamDouble, candidate: Double) {
    if (candidate.isNaN()) return
    while (true) {
        val current = cell.load()
        if (candidate >= current) return
        if (cell.compareAndSet(current, candidate)) return
    }
}

internal fun casOr(cell: StreamLong, bitMask: Long) {
    while (true) {
        val current = cell.load()
        val updated = current or bitMask
        if (current == updated) return
        if (cell.compareAndSet(current, updated)) return
    }
}

internal fun casMax(arr: StreamLongArray, index: Int, candidate: Long) {
    while (true) {
        val current = arr.load(index)
        if (candidate <= current) return
        if (arr.compareAndSet(index, current, candidate)) return
    }
}

internal fun casMin(arr: StreamLongArray, index: Int, candidate: Long) {
    while (true) {
        val current = arr.load(index)
        if (candidate >= current) return
        if (arr.compareAndSet(index, current, candidate)) return
    }
}

internal fun casOr(arr: StreamLongArray, index: Int, bitMask: Long) {
    while (true) {
        val current = arr.load(index)
        val updated = current or bitMask
        if (current == updated) return
        if (arr.compareAndSet(index, current, updated)) return
    }
}
