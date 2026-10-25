package com.eignex.kumulant.core

// Bound array output so logging a large sketch does not dump its entire backing storage.
private const val PREVIEW_LIMIT = 16

internal fun DoubleArray.preview(): String = if (size <= PREVIEW_LIMIT) contentToString() else "DoubleArray(size=$size)"

internal fun IntArray.preview(): String = if (size <= PREVIEW_LIMIT) contentToString() else "IntArray(size=$size)"

internal fun LongArray.preview(): String = if (size <= PREVIEW_LIMIT) contentToString() else "LongArray(size=$size)"
