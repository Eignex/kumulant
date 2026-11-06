package com.eignex.kumulant.math

import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

/**
 * SplitMix64 - a fast, high-quality 64-bit mixer suitable for spreading sequential
 * or low-entropy keys into a uniform 64-bit hash before feeding them to cardinality
 * sketches. Output passes BigCrush; not collision-resistant (use a cryptographic hash
 * if adversarial input is a concern).
 */
fun splitmix64(value: Long): Long = com.eignex.util.hash.splitmix64(value)

// Mixing the child ordinal separates replica streams from the parent draw sequence.
internal fun deriveChildSeed(seed: Long, ordinal: Long): Long = splitmix64(seed xor splitmix64(ordinal))

/**
 * 64-bit hash of [bytes] for cardinality / sketch families: SplitMix64 over 8-byte
 * little-endian chunks, folding tail bytes in last and seeding with the input length so
 * zero-prefixed inputs of different lengths hash distinctly. Stable byte-for-byte across
 * platforms.
 *
 * Prefer this over `value.hashCode().toLong()` when feeding HLL,
 * [BloomFilterStat][com.eignex.kumulant.stat.sketch.BloomFilterStat], or
 * [CountMinSketchStat][com.eignex.kumulant.stat.sketch.CountMinSketchStat] - those rely on
 * uniform 64-bit entropy, and JVM `hashCode` only provides 32 bits.
 *
 * Not collision-resistant. Use a cryptographic hash for adversarial input.
 */
fun hash64(bytes: ByteArray): Long = com.eignex.util.hash.hash64(bytes)

/** UTF-8 byte hash convenience over [hash64]. */
fun hash64(value: String): Long = com.eignex.util.hash.hash64(value)

/**
 * Pluggable `Long -> Long` mixer used by the discrete sketch family (HyperLogLog,
 * BloomFilter, CountMinSketch) to spread a key's bits across
 * the full 64-bit range before bucketing. Distinct from [hash64] (`ByteArray -> Long`):
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

private const val SPLIT_MIX64_NAME: String = "splitmix64"

/** Default [LongHasher]: the library's [splitmix64] mixer. Pre-registered with [Hashers]. */
val SplitMix64: LongHasher = object : LongHasher {
    override val name: String = SPLIT_MIX64_NAME
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
    /** Built-in [HasherRef] constants. */
    companion object {
        /** Reference to the default [SplitMix64] mixer. */
        val SplitMix64: HasherRef = HasherRef(SPLIT_MIX64_NAME)
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
