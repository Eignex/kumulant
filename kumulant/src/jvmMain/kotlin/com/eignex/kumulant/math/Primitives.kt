package com.eignex.kumulant.math

import jdk.incubator.vector.DoubleVector
import jdk.incubator.vector.VectorOperators

/**
 * JVM dense primitives. Routes to a SIMD path (`jdk.incubator.vector`) when the
 * incubator module is available at runtime, scalar fallback otherwise.
 *
 * Detection runs once at class load via `Class.forName`. The SIMD code lives in a
 * separate private object ([Simd]) so its static initializer (which references
 * `DoubleVector`) only runs when the probe succeeds. A JVM started without
 * `--add-modules=jdk.incubator.vector` loads this file cleanly and takes the
 * scalar path.
 *
 * Build wiring: kumulant compiles with `-Xadd-modules=jdk.incubator.vector` to
 * make the SIMD code resolve at compile time. Tests pass the same flag at runtime
 * to exercise the SIMD path; consumers who pass it get SIMD, consumers who don't
 * get scalar. No extra config required for correctness.
 */

private val simdAvailable: Boolean = try {
    Class.forName("jdk.incubator.vector.DoubleVector")
    true
} catch (_: Throwable) {
    false
}

internal actual fun denseDot(
    a: DoubleArray,
    aOff: Int,
    b: DoubleArray,
    bOff: Int,
    len: Int,
): Double = if (simdAvailable) Simd.dot(a, aOff, b, bOff, len) else scalarDot(a, aOff, b, bOff, len)

internal actual fun denseAxpy(
    y: DoubleArray,
    yOff: Int,
    alpha: Double,
    x: DoubleArray,
    xOff: Int,
    len: Int,
) {
    if (simdAvailable) Simd.axpy(y, yOff, alpha, x, xOff, len) else scalarAxpy(y, yOff, alpha, x, xOff, len)
}

internal actual fun denseScale(
    v: DoubleArray,
    vOff: Int,
    alpha: Double,
    len: Int,
) {
    if (simdAvailable) Simd.scale(v, vOff, alpha, len) else scalarScale(v, vOff, alpha, len)
}

/** Identifies the runtime math backend powering the SIMD-like primitives. */
public actual val mathBackend: String = if (simdAvailable) "simd(${Simd.lanes()} lanes)" else "scalar"

// === Scalar fallback (matches nonJvmMain's implementations) ================

private fun scalarDot(a: DoubleArray, aOff: Int, b: DoubleArray, bOff: Int, len: Int): Double {
    var s = 0.0
    for (i in 0 until len) s += a[aOff + i] * b[bOff + i]
    return s
}

private fun scalarAxpy(y: DoubleArray, yOff: Int, alpha: Double, x: DoubleArray, xOff: Int, len: Int) {
    if (alpha == 0.0) return
    for (i in 0 until len) y[yOff + i] += alpha * x[xOff + i]
}

private fun scalarScale(v: DoubleArray, vOff: Int, alpha: Double, len: Int) {
    if (alpha == 1.0) return
    for (i in 0 until len) v[vOff + i] *= alpha
}

// === SIMD path - only loaded when simdAvailable is true ====================

private object Simd {
    private val SPECIES = DoubleVector.SPECIES_PREFERRED
    private val LANE = SPECIES.length()

    fun lanes(): Int = LANE

    fun dot(a: DoubleArray, aOff: Int, b: DoubleArray, bOff: Int, len: Int): Double {
        var i = 0
        val bound = SPECIES.loopBound(len)
        var sum = DoubleVector.zero(SPECIES)
        while (i < bound) {
            val va = DoubleVector.fromArray(SPECIES, a, aOff + i)
            val vb = DoubleVector.fromArray(SPECIES, b, bOff + i)
            sum = va.fma(vb, sum)
            i += LANE
        }
        var s = sum.reduceLanes(VectorOperators.ADD)
        while (i < len) {
            s += a[aOff + i] * b[bOff + i]
            i++
        }
        return s
    }

    fun axpy(y: DoubleArray, yOff: Int, alpha: Double, x: DoubleArray, xOff: Int, len: Int) {
        if (alpha == 0.0) return
        val alphaVec = DoubleVector.broadcast(SPECIES, alpha)
        var i = 0
        val bound = SPECIES.loopBound(len)
        while (i < bound) {
            val vx = DoubleVector.fromArray(SPECIES, x, xOff + i)
            val vy = DoubleVector.fromArray(SPECIES, y, yOff + i)
            // y_new = alpha * x + y  ->  vx.fma(alphaVec, vy) computes vx * alphaVec + vy.
            vx.fma(alphaVec, vy).intoArray(y, yOff + i)
            i += LANE
        }
        while (i < len) {
            y[yOff + i] += alpha * x[xOff + i]
            i++
        }
    }

    fun scale(v: DoubleArray, vOff: Int, alpha: Double, len: Int) {
        if (alpha == 1.0) return
        val alphaVec = DoubleVector.broadcast(SPECIES, alpha)
        var i = 0
        val bound = SPECIES.loopBound(len)
        while (i < bound) {
            val vv = DoubleVector.fromArray(SPECIES, v, vOff + i)
            vv.mul(alphaVec).intoArray(v, vOff + i)
            i += LANE
        }
        while (i < len) {
            v[vOff + i] *= alpha
            i++
        }
    }
}
