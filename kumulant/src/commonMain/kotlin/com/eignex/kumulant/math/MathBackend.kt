package com.eignex.kumulant.math

/**
 * Short human-readable identifier for the math backend the current process resolved.
 * Examples: `"scalar"` (any non-JVM target, or a JVM started without
 * `--add-modules=jdk.incubator.vector`), `"simd(4 lanes)"` (JVM with AVX2),
 * `"simd(8 lanes)"` (JVM with AVX-512). Print at startup to verify your runtime
 * picked up the Vector API module.
 */
public expect val mathBackend: String
