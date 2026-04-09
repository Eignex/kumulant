package com.eignex.kumulant.concurrent

private val threadLocalMode = ThreadLocal.withInitial<StreamMode> { SerialMode }

actual var defaultStreamMode: StreamMode
    get() = threadLocalMode.get()
    set(value) = threadLocalMode.set(value)
