package com.eignex.kumulant.math

import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

// 64-bit hashing primitives used by the cardinality and sketch families.
//
// Naming convention for additional algorithms: each algorithm exposes top-level
// functions named `<algo>64`, e.g. [splitmix64], `xxhash64(ByteArray)`,
// `wyhash64(ByteArray)`. The unqualified [hash64] is an alias for the library's
// recommended default `ByteArray` hash; callers that don't care which algorithm
// produces the bits should use it. Callers needing a stable byte stream across
// library versions should pin to a specific named algorithm or [Hasher64].

/**
 * SplitMix64 - a fast, high-quality 64-bit mixer suitable for spreading sequential
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
 * delegates to [SplitMixChunkHasher] - pin to that hasher directly if you need a
 * stable byte stream across library versions.
 *
 * Prefer this over `value.hashCode().toLong()` when feeding HLL,
 * [MinHashStat][com.eignex.kumulant.stat.sketch.MinHashStat],
 * [BloomFilterStat][com.eignex.kumulant.stat.sketch.BloomFilterStat], or
 * [CountMinSketchStat][com.eignex.kumulant.stat.sketch.CountMinSketchStat] - those rely on
 * uniform 64-bit entropy, and JVM `hashCode` only provides 32 bits.
 *
 * Not collision-resistant. Use a cryptographic hash for adversarial input.
 */
fun hash64(bytes: ByteArray): Long = SplitMixChunkHasher.hash(bytes)

/** UTF-8 byte hash convenience over [hash64]. */
fun hash64(value: String): Long = hash64(value.encodeToByteArray())

/** Pluggable 64-bit byte hash. Implementations must be deterministic and pure. */
fun interface Hasher64 {
    /** Return a 64-bit hash of [bytes]. Equal byte arrays must return equal hashes. */
    fun hash(bytes: ByteArray): Long
}

/**
 * Pluggable `Long -> Long` mixer used by the discrete sketch family (HyperLogLog,
 * LinearCounting, MinHash, BloomFilter, CountMinSketch) to spread a key's bits across
 * the full 64-bit range before bucketing. Distinct from [Hasher64] (`ByteArray -> Long`):
 * callers reduce a domain key to a `Long` first (e.g. via [hash64]), and the sketch then
 * mixes that `Long` through here.
 *
 * Implementations must be deterministic and pure, and expose a stable [name] so a sketch
 * can record which mixer produced its summary on the wire. Register a custom mixer with
 * [Hashers.register] so [Hashers.resolve] can rebuild it from a name after deserialization.
 */
interface LongHasher {
    /** Stable identifier serialized into specs and results, and used as the registry key. */
    val name: String

    /** Mix [value] into a uniformly spread 64-bit hash. */
    fun mix(value: Long): Long
}

/** Canonical name of the default [LongHasher]; the wire default for the sketch specs. */
const val SplitMix64Name: String = "splitmix64"

/** Default [LongHasher]: the library's [splitmix64] mixer. Pre-registered with [Hashers]. */
val SplitMix64: LongHasher = object : LongHasher {
    override val name: String = SplitMix64Name
    override fun mix(value: Long): Long = splitmix64(value)
}

/**
 * Typed, serializable reference to a [LongHasher] by [name]. Carried by the discrete sketch
 * specs and results in place of a bare string, and resolved to a live mixer via
 * [Hashers.resolve]. Serializes transparently as its [name], so the wire form stays a plain
 * string and needs no custom serializer.
 */
@Serializable
@JvmInline
value class HasherRef(val name: String) {
    companion object {
        /** Reference to the default [SplitMix64] mixer. */
        val SplitMix64: HasherRef = HasherRef(SplitMix64Name)
    }
}

/**
 * Registry resolving a [LongHasher.name] back to its live implementation. The sketch
 * families serialize only the mixer's name; [resolve] reconstructs the function when a
 * spec is materialized or a sketch result is queried. [SplitMix64] is pre-registered.
 *
 * Custom mixers are not serializable on their own (a lambda cannot round-trip), so the
 * wire carries the name and this registry supplies the code. Register a custom mixer at
 * startup on every process that materializes or queries the affected sketches; the entry
 * is global and not synchronized, so register before concurrent use.
 */
object Hashers {
    private val registry: MutableMap<String, LongHasher> = mutableMapOf(SplitMix64.name to SplitMix64)

    /** Register [hasher] under its [LongHasher.name], replacing any prior entry of that name. */
    fun register(hasher: LongHasher) {
        registry[hasher.name] = hasher
    }

    /** Resolve the [LongHasher] named by [ref], or throw if no mixer was registered under it. */
    fun resolve(ref: HasherRef): LongHasher =
        registry[ref.name] ?: error("Unknown LongHasher '${ref.name}'; register it via Hashers.register before use")
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
        // Always finalize with a tail mix - even when bytes.size is a multiple of 8 -
        // so chunk-aligned and unaligned inputs that happen to reach the same
        // intermediate state cannot collide.
        var tail = 0L
        var shift = 0
        while (i < bytes.size) {
            tail = tail or ((bytes[i].toLong() and 0xFF) shl shift)
            shift += 8
            i++
        }
        return splitmix64(h xor tail)
    }
}
