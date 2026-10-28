package com.eignex.kumulant.stream

// Striped adders are JVM-only; other platforms use atomic cells for additive HighWrite stats.
internal expect val highWriteMode: StreamMode
