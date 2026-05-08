package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertNotSame
import kotlin.test.assertSame

class AdditiveModeTest {

    @Test
    fun `None returns SerialMode`() {
        assertSame(SerialMode, Concurrency.None.additiveMode())
    }

    @Test
    fun `Relaxed returns AtomicMode`() {
        assertSame(AtomicMode, Concurrency.Relaxed.additiveMode())
    }

    @Test
    fun `Strict returns AtomicMode`() {
        assertSame(AtomicMode, Concurrency.Strict.additiveMode())
    }

    @Test
    fun `HighWrite is striped not Serial`() {
        // JVM picks AdderMode (in jvmMain, not visible here); other targets
        // fall back to AtomicMode. Either way, it's not SerialMode.
        assertNotSame(SerialMode, Concurrency.HighWrite.additiveMode())
    }
}

class MonotonicModeTest {

    @Test
    fun `None returns SerialMode`() {
        assertSame(SerialMode, Concurrency.None.monotonicMode())
    }

    @Test
    fun `Relaxed returns AtomicMode`() {
        assertSame(AtomicMode, Concurrency.Relaxed.monotonicMode())
    }

    @Test
    fun `Strict returns AtomicMode`() {
        assertSame(AtomicMode, Concurrency.Strict.monotonicMode())
    }

    @Test
    fun `HighWrite returns AtomicMode`() {
        assertSame(AtomicMode, Concurrency.HighWrite.monotonicMode())
    }
}

class WelfordModeTest {

    @Test
    fun `None returns SerialMode`() {
        assertSame(SerialMode, Concurrency.None.welfordMode())
    }

    @Test
    fun `Relaxed returns AtomicMode`() {
        assertSame(AtomicMode, Concurrency.Relaxed.welfordMode())
    }

    @Test
    fun `Strict returns SerialMode`() {
        // Strict locks at the stat level — cells then drop atomic overhead.
        assertSame(SerialMode, Concurrency.Strict.welfordMode())
    }

    @Test
    fun `HighWrite returns SerialMode`() {
        assertSame(SerialMode, Concurrency.HighWrite.welfordMode())
    }
}

class WelfordLockTest {

    @Test
    fun `None returns NoopMutex`() {
        assertSame(NoopMutex, Concurrency.None.welfordLock())
    }

    @Test
    fun `Relaxed returns NoopMutex`() {
        assertSame(NoopMutex, Concurrency.Relaxed.welfordLock())
    }

    @Test
    fun `Strict returns PlatformMutex`() {
        val lock = Concurrency.Strict.welfordLock()
        assertNotSame(NoopMutex, lock)
        kotlin.test.assertTrue(lock is PlatformMutex)
    }

    @Test
    fun `HighWrite returns PlatformMutex`() {
        val lock = Concurrency.HighWrite.welfordLock()
        kotlin.test.assertTrue(lock is PlatformMutex)
    }
}

class SerializedLockTest {

    @Test
    fun `None returns NoopMutex`() {
        assertSame(NoopMutex, Concurrency.None.serializedLock())
    }

    @Test
    fun `Relaxed returns PlatformMutex`() {
        val lock = Concurrency.Relaxed.serializedLock()
        kotlin.test.assertTrue(lock is PlatformMutex)
    }

    @Test
    fun `Strict returns PlatformMutex`() {
        val lock = Concurrency.Strict.serializedLock()
        kotlin.test.assertTrue(lock is PlatformMutex)
    }

    @Test
    fun `HighWrite returns PlatformMutex`() {
        val lock = Concurrency.HighWrite.serializedLock()
        kotlin.test.assertTrue(lock is PlatformMutex)
    }
}
