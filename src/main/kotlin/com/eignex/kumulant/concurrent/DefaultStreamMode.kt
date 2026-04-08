package com.eignex.kumulant.concurrent

private val threadLocalMode = ThreadLocal.withInitial<StreamMode> { SerialMode }

var defaultStreamMode: StreamMode
    get() = threadLocalMode.get()
    set(value) = threadLocalMode.set(value)

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
