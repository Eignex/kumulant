package com.eignex.kumulant.stream

/**
 * Tiny CAS spin-mutex for stat classes whose update logic is inherently multi-step
 * (TDigest's compress, Reservoir/SpaceSaving's admit/evict) and therefore can't be
 * made elementwise atomic. Backed by a [StreamLong] from the owning mode so the
 * primitive is KMP-portable and respects the mode's atomicity contract.
 *
 * Uncontested CAS is essentially free (one atomic op), so wrapping every
 * `update`/`read` is fine on the single-threaded `SerialMode` path. Under
 * contention it spins — acceptable for short critical sections, not great for
 * long ones. Upgrade to a yielding lock (atomicfu `SynchronizedObject`, or an
 * `expect/actual synchronized`) once one is available; the [withLock] call sites
 * stay unchanged.
 */
internal class CasLock(mode: StreamMode) {
    @PublishedApi
    internal val flag: StreamLong = mode.newLong(0L)

    inline fun <R> withLock(block: () -> R): R {
        while (!flag.compareAndSet(0L, 1L)) {
            // spin
        }
        try {
            return block()
        } finally {
            flag.store(0L)
        }
    }
}
