package com.eignex.kumulant.math

/**
 * Internal arithmetic primitives over [VectorView] / [MatrixView].
 *
 * **Sparse-aware iteration.** Every operation that needs to iterate the entries of
 * a vector goes through [forEachStored], which dispatches dense→all-indices and
 * sparse→stored-entries. Hot paths inline; cold paths trade a bit of overhead for
 * a single tested iteration surface.
 *
 * **SIMD hot paths.** Dense×dense paths delegate to the [denseDot] / [denseAxpy] /
 * [denseScale] primitives defined in `Primitives.kt`. JVM implementations use the
 * incubator Vector API; non-JVM targets use scalar fallbacks.
 *
 * **In-place vs fresh.** Functions that mutate a destination take it as the first
 * argument and return [Unit] (`scale`, `axpy`, `addOuter`). Functions that allocate
 * a fresh result return it directly (`matVec`, infix `dot`).
 *
 * Everything in this file is `internal` so the public vector/matrix surface stays
 * read-only.
 */

// === Iteration ============================================================

/**
 * Visit each stored entry of [this] as `(index, value)`. For [DenseVector] that's
 * every index in `0 until size`; for [SparseVector] that's the entries present in
 * the parallel index/value arrays (which may include numerical zeros).
 */
internal inline fun VectorView.forEachStored(block: (i: Int, v: Double) -> Unit) {
    when (this) {
        is DenseVector -> {
            val d = data
            for (i in 0 until d.size) block(i, d[i])
        }
        is SparseVector -> {
            val idx = indices
            val vals = values
            for (k in idx.indices) block(idx[k], vals[k])
        }
    }
}

// === Scalar dispatch =======================================================

/** `aᵀ · b`. Dense×dense routes through [denseDot] (SIMD on JVM); sparse paths
 *  iterate the cheaper operand's stored entries. */
internal infix fun VectorView.dot(other: VectorView): Double {
    require(size == other.size) { "size mismatch: $size vs ${other.size}" }
    if (this is DenseVector && other is DenseVector) {
        return denseDot(data, 0, other.data, 0, size)
    }
    // At least one sparse — iterate that side, gather from the other.
    return if (this is SparseVector || other !is SparseVector) {
        var s = 0.0
        this.forEachStored { i, v -> s += v * other[i] }
        s
    } else {
        var s = 0.0
        other.forEachStored { i, v -> s += v * this[i] }
        s
    }
}

// === In-place vector ops ===================================================

/** `y ← y + α · x`. Dense `x` uses SIMD; sparse `x` walks stored entries. */
internal fun axpy(y: DenseVector, alpha: Double, x: VectorView) {
    require(y.size == x.size) { "size mismatch: ${y.size} vs ${x.size}" }
    if (alpha == 0.0) return
    if (x is DenseVector) {
        denseAxpy(y.data, 0, alpha, x.data, 0, y.size)
    } else {
        val yd = y.data
        x.forEachStored { i, v -> yd[i] += alpha * v }
    }
}

/** `v ← α · v`. */
internal fun scale(v: DenseVector, alpha: Double) {
    if (alpha == 1.0) return
    denseScale(v.data, 0, alpha, v.size)
}

// === In-place matrix ops ===================================================

/**
 * `M ← M + α · x · yᵀ` (rank-1 update). Subtract by passing `alpha = -1.0`.
 *
 * Dense×dense routes each row's update through [denseAxpy] (SIMD). Sparse paths
 * only visit the rows/cols where `x_i · y_j` could be non-zero.
 */
internal fun addOuter(M: DenseMatrix, alpha: Double, x: VectorView, y: VectorView) {
    require(M.rows == x.size && M.cols == y.size) {
        "addOuter shape mismatch: M is ${M.rows}x${M.cols}, x ${x.size}, y ${y.size}"
    }
    if (alpha == 0.0) return
    val md = M.data
    val cols = M.cols
    if (x is DenseVector && y is DenseVector) {
        val xd = x.data
        for (i in 0 until M.rows) {
            val xi = xd[i]
            if (xi != 0.0) denseAxpy(md, i * cols, alpha * xi, y.data, 0, cols)
        }
        return
    }
    // Mixed or sparse — fall back to per-stored-entry updates.
    x.forEachStored { i, xi ->
        if (xi != 0.0) {
            val row = i * cols
            val scaled = alpha * xi
            y.forEachStored { j, yj -> md[row + j] += scaled * yj }
        }
    }
}

// === Allocating ops ========================================================

/** Matrix-vector product `A · x` into a fresh dense result. */
internal fun matVec(A: MatrixView, x: VectorView): DenseVector {
    require(A.cols == x.size) { "matVec shape mismatch: A is ${A.rows}x${A.cols}, x size ${x.size}" }
    val out = DenseVector(A.rows)
    val od = out.data
    if (A is DenseMatrix) {
        val ad = A.data
        val cols = A.cols
        if (x is DenseVector) {
            for (i in 0 until A.rows) od[i] = denseDot(ad, i * cols, x.data, 0, cols)
        } else {
            for (i in 0 until A.rows) {
                val row = i * cols
                var s = 0.0
                x.forEachStored { j, v -> s += ad[row + j] * v }
                od[i] = s
            }
        }
    } else {
        for (i in 0 until A.rows) {
            var s = 0.0
            x.forEachStored { j, v -> s += A[i, j] * v }
            od[i] = s
        }
    }
    return out
}
