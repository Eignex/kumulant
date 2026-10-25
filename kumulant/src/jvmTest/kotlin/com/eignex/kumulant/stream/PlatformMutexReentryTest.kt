package com.eignex.kumulant.stream

import kotlin.test.Test
import kotlin.test.assertFailsWith

class PlatformMutexReentryTest {

    // Requires assertions enabled with -ea, as in the Gradle test task.
    @Test
    fun `re-acquiring a held lock is reported rather than granted`() {
        val mutex = PlatformMutex()
        assertFailsWith<AssertionError> {
            mutex.guarded {
                mutex.guarded { }
            }
        }
    }
}
