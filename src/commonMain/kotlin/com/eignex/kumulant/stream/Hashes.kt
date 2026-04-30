package com.eignex.kumulant.stream

/**
 * 64-bit hashing primitives used by the cardinality and sketch families.
 *
 * Naming convention for additional algorithms: each algorithm exposes top-level
 * functions named `<algo>64`, e.g. [splitmix64], `xxhash64(ByteArray)`,
 * `wyhash64(ByteArray)`. The unqualified [hash64] is an alias for the library's
 * recommended default `ByteArray` hash; callers that don't care which algorithm
 * produces the bits should use it. Callers needing a stable byte stream across
 * library versions should pin to a specific named algorithm or [Hasher64].
 */

/**
 * SplitMix64 — a fast, high-quality 64-bit mixer suitable for spreading sequential
 * or low-entropy keys into a uniform 64-bit hash before feeding them to cardinality
 * sketches. Output passes BigCrush; not collision-resistant (use a cryptographic hash
 * if adversarial input is a concern).
 */
fun splitmix64(value: Long): Long {
    var z = value + -7046029254386353133L // 0x9E3779B97F4A7C15
    z = (z xor (z ushr 30)) * -4658895280553007687L // 0xBF58476D1CE4E5B9
    z = (z xor (z ushr 27)) * -6534734903238641935L // 0x94D049BB133111EB
    return z xor (z ushr 31)
}

/**
 * Default 64-bit hash of [bytes] for cardinality / sketch families. Currently
 * delegates to [SplitMixChunkHasher] — pin to that hasher directly if you need a
 * stable byte stream across library versions.
 *
 * Prefer this over `value.hashCode().toLong()` when feeding HLL, MinHash, BloomFilter,
 * or CountMinSketch — those rely on uniform 64-bit entropy, and JVM `hashCode` only
 * provides 32 bits.
 *
 * Not collision-resistant. Use a cryptographic hash for adversarial input.
 */
fun hash64(bytes: ByteArray): Long = SplitMixChunkHasher.hash(bytes)

/** UTF-8 byte hash convenience over [hash64]. */
fun hash64(value: String): Long = hash64(value.encodeToByteArray())

/** Pluggable 64-bit byte hash. Implementations must be deterministic and pure. */
fun interface Hasher64 {
    fun hash(bytes: ByteArray): Long
}

/**
 * Hashes byte arrays by feeding 8-byte little-endian chunks through [splitmix64]
 * and folding tail bytes in last. The starting state is the input length, so
 * different-length zero-prefixed inputs hash distinctly. Stable byte-for-byte
 * across platforms; currently the default for [hash64].
 */
object SplitMixChunkHasher : Hasher64 {
    override fun hash(bytes: ByteArray): Long {
        var h = bytes.size.toLong()
        var i = 0
        while (i + 8 <= bytes.size) {
            var chunk = 0L
            for (j in 0 until 8) {
                chunk = chunk or ((bytes[i + j].toLong() and 0xFF) shl (j * 8))
            }
            h = splitmix64(h xor chunk)
            i += 8
        }
        if (i < bytes.size) {
            var tail = 0L
            var shift = 0
            while (i < bytes.size) {
                tail = tail or ((bytes[i].toLong() and 0xFF) shl shift)
                shift += 8
                i++
            }
            h = splitmix64(h xor tail)
        }
        return h
    }
}
