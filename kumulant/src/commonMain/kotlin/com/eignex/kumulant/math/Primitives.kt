package com.eignex.kumulant.math

// Platform-dispatched dense-vector primitives - `internal` building blocks that
// higher-level ops (`dot`, `axpy`, `matVec`, `cholesky`) call on contiguous
// `DoubleArray` runs. JVM provides a SIMD implementation via the incubator
// `jdk.incubator.vector` API; every other target uses a scalar fallback in
// `nonJvmMain`.
//
// All primitives take `DoubleArray` plus an `offset` and `length` so the same
// call site works for whole vectors, matrix rows, or sub-slices.

/** `Sum a[aOff..aOff+len-1] * b[bOff..bOff+len-1]`. */
internal expect fun denseDot(a: DoubleArray, aOff: Int, b: DoubleArray, bOff: Int, len: Int): Double

/** `y[yOff..] = y[yOff..] + alpha * x[xOff..]`. */
internal expect fun denseAxpy(y: DoubleArray, yOff: Int, alpha: Double, x: DoubleArray, xOff: Int, len: Int)

/** `v[vOff..vOff+len-1] = alpha * v[..]`. */
internal expect fun denseScale(v: DoubleArray, vOff: Int, alpha: Double, len: Int)

/**
 * Short human-readable identifier for the math backend the current process resolved.
 * Examples: `"scalar"` (any non-JVM target, or a JVM started without
 * `--add-modules=jdk.incubator.vector`), `"simd(4 lanes)"` (JVM with AVX2),
 * `"simd(8 lanes)"` (JVM with AVX-512). Print at startup to verify your runtime
 * picked up the Vector API module.
 */
public expect val mathBackend: String
