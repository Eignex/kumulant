package com.eignex.kumulant.math

/**
 * Scalar fallback for the dense primitives. The JVM target overrides these with a
 * SIMD-backed implementation using `jdk.incubator.vector`; non-JVM targets
 * (native, JS, Wasm) use the loops below - competitive with hand-written code
 * once the platform's compiler vectorises the inner loop, but without the
 * guaranteed lane-width win that the Vector API delivers on JVM.
 */

internal actual fun denseDot(
    a: DoubleArray,
    aOff: Int,
    b: DoubleArray,
    bOff: Int,
    len: Int,
): Double {
    var s = 0.0
    for (i in 0 until len) s += a[aOff + i] * b[bOff + i]
    return s
}

internal actual fun denseAxpy(
    y: DoubleArray,
    yOff: Int,
    alpha: Double,
    x: DoubleArray,
    xOff: Int,
    len: Int,
) {
    if (alpha == 0.0) return
    for (i in 0 until len) y[yOff + i] += alpha * x[xOff + i]
}

internal actual fun denseScale(
    v: DoubleArray,
    vOff: Int,
    alpha: Double,
    len: Int,
) {
    if (alpha == 1.0) return
    for (i in 0 until len) v[vOff + i] *= alpha
}

/** Identifies the runtime math backend powering the SIMD-like primitives. */
public actual val mathBackend: String = "scalar"
