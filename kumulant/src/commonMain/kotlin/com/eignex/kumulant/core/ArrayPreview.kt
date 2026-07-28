package com.eignex.kumulant.core

/**
 * Rendering helpers for the array-backed fields of [Result] types.
 *
 * Results are routinely logged, and some of them carry large arrays: a HyperLogLog register
 * bank is `2^precision` entries, a Count-Min sketch is `depth * width`. Dumping those in
 * full turns one log line into thousands, so these render the contents only when the array
 * is small enough to be worth reading and fall back to a type-and-length summary otherwise.
 */
private const val PREVIEW_LIMIT = 16

internal fun DoubleArray.preview(): String = if (size <= PREVIEW_LIMIT) contentToString() else "DoubleArray(size=$size)"

internal fun IntArray.preview(): String = if (size <= PREVIEW_LIMIT) contentToString() else "IntArray(size=$size)"

internal fun LongArray.preview(): String = if (size <= PREVIEW_LIMIT) contentToString() else "LongArray(size=$size)"
