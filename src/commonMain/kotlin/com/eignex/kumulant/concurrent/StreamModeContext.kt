package com.eignex.kumulant.concurrent

/**
 * Temporarily overrides the default mode for the duration of the block.
 */
fun <T> withMode(mode: StreamMode, block: () -> T): T {
    val previousMode = defaultStreamMode
    defaultStreamMode = mode
    return try {
        block()
    } finally {
        defaultStreamMode = previousMode
    }
}
