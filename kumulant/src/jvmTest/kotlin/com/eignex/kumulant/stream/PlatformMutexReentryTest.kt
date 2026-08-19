package com.eignex.kumulant.stream

import kotlin.test.Test
import kotlin.test.assertFailsWith

class PlatformMutexReentryTest {

    // Rests on -ea, which Gradle's test task enables by default; without it the check compiles out
    // and there is nothing to catch.
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
