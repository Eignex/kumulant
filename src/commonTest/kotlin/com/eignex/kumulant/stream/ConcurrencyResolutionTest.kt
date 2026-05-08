package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertNotSame
import kotlin.test.assertSame

class AdditiveModeTest {

    @Test
    fun `additiveMode should return SerialMode for None`() {
        assertSame(SerialMode, Concurrency.None.additiveMode())
    }

    @Test
    fun `additiveMode should return AtomicMode for Relaxed`() {
        assertSame(AtomicMode, Concurrency.Relaxed.additiveMode())
    }

    @Test
    fun `additiveMode should return AtomicMode for Strict`() {
        assertSame(AtomicMode, Concurrency.Strict.additiveMode())
    }

    @Test
    fun `additiveMode should pick striped not Serial for HighWrite`() {
        // JVM picks AdderMode (in jvmMain, not visible here); other targets
        // fall back to AtomicMode. Either way it's not SerialMode.
        assertNotSame(SerialMode, Concurrency.HighWrite.additiveMode())
    }
}

class MonotonicModeTest {

    @Test
    fun `monotonicMode should return SerialMode for None`() {
        assertSame(SerialMode, Concurrency.None.monotonicMode())
    }

    @Test
    fun `monotonicMode should return AtomicMode for Relaxed`() {
        assertSame(AtomicMode, Concurrency.Relaxed.monotonicMode())
    }

    @Test
    fun `monotonicMode should return AtomicMode for Strict`() {
        assertSame(AtomicMode, Concurrency.Strict.monotonicMode())
    }

    @Test
    fun `monotonicMode should return AtomicMode for HighWrite`() {
        assertSame(AtomicMode, Concurrency.HighWrite.monotonicMode())
    }
}

class WelfordModeTest {

    @Test
    fun `welfordMode should return SerialMode for None`() {
        assertSame(SerialMode, Concurrency.None.welfordMode())
    }

    @Test
    fun `welfordMode should return AtomicMode for Relaxed`() {
        assertSame(AtomicMode, Concurrency.Relaxed.welfordMode())
    }

    @Test
    fun `welfordMode should return SerialMode for Strict`() {
        assertSame(SerialMode, Concurrency.Strict.welfordMode())
    }

    @Test
    fun `welfordMode should return SerialMode for HighWrite`() {
        assertSame(SerialMode, Concurrency.HighWrite.welfordMode())
    }
}

class WelfordLockTest {

    @Test
    fun `welfordLock should return NoopMutex for None`() {
        assertSame(NoopMutex, Concurrency.None.welfordLock())
    }

    @Test
    fun `welfordLock should return NoopMutex for Relaxed`() {
        assertSame(NoopMutex, Concurrency.Relaxed.welfordLock())
    }

    @Test
    fun `welfordLock should return PlatformMutex for Strict`() {
        kotlin.test.assertTrue(Concurrency.Strict.welfordLock() is PlatformMutex)
    }

    @Test
    fun `welfordLock should return PlatformMutex for HighWrite`() {
        kotlin.test.assertTrue(Concurrency.HighWrite.welfordLock() is PlatformMutex)
    }
}

class SerializedLockTest {

    @Test
    fun `serializedLock should return NoopMutex for None`() {
        assertSame(NoopMutex, Concurrency.None.serializedLock())
    }

    @Test
    fun `serializedLock should return PlatformMutex for Relaxed`() {
        kotlin.test.assertTrue(Concurrency.Relaxed.serializedLock() is PlatformMutex)
    }

    @Test
    fun `serializedLock should return PlatformMutex for Strict`() {
        kotlin.test.assertTrue(Concurrency.Strict.serializedLock() is PlatformMutex)
    }

    @Test
    fun `serializedLock should return PlatformMutex for HighWrite`() {
        kotlin.test.assertTrue(Concurrency.HighWrite.serializedLock() is PlatformMutex)
    }
}
