package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.preview
import com.eignex.kumulant.math.HasherRef
import com.eignex.kumulant.math.Hashers
import com.eignex.kumulant.math.LongHasher
import com.eignex.kumulant.math.SplitMix64
import com.eignex.kumulant.math.splitmix64
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamLongArray
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * CountStat-MinStat sketch snapshot. [counters] is the [depth] x [width] matrix of counters in
 * row-major order. [seed] determines the per-row hash salts; merging two snapshots
 * requires identical [depth], [width], and [seed]. [totalSeen] is the unweighted update
 * count.
 */
@Serializable
@SerialName("CountMinSketchResult")
data class CountMinSketchResult(
    val depth: Int,
    val width: Int,
    val seed: Long,
    val counters: LongArray,
    val totalSeen: Long,
    /** Reference to the [LongHasher] that produced the counters; resolved by [estimate]. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : HasObservationCount {

    override val totalWeights: Double get() = totalSeen.toDouble()
    override fun equals(other: Any?): Boolean = other is CountMinSketchResult &&
        depth == other.depth &&
        width == other.width &&
        seed == other.seed &&
        counters.contentEquals(other.counters) &&
        totalSeen == other.totalSeen &&
        hasher == other.hasher

    override fun hashCode(): Int {
        var h = depth.hashCode()
        h = 31 * h + width.hashCode()
        h = 31 * h + seed.hashCode()
        h = 31 * h + counters.contentHashCode()
        h = 31 * h + totalSeen.hashCode()
        h = 31 * h + hasher.hashCode()
        return h
    }

    override fun toString(): String = "CountMinSketchResult(" +
        "depth=$depth, " +
        "width=$width, " +
        "seed=$seed, " +
        "counters=${counters.preview()}, " +
        "totalSeen=$totalSeen, " +
        "hasher=$hasher)"
}

/**
 * Estimated weighted count of [value] - the minimum across rows. Re-derives the per-row
 * index with the same [LongHasher] the sketch used (resolved by name via [Hashers]), so a
 * custom hasher must be registered before querying.
 */
fun CountMinSketchResult.estimate(value: Long): Long {
    if (counters.isEmpty()) return 0L
    val mask = (width - 1).toLong()
    val mixer = Hashers.resolve(hasher)
    // No sentinel: Long.MAX_VALUE is a value a counter can legitimately hold (a saturating weight
    // puts it there), so treating it as "no rows visited" would report 0 for the largest count
    // representable. depth is always positive, so row 0 seeds the minimum.
    var min = Long.MAX_VALUE
    for (row in 0 until depth) {
        val salt = splitmix64(seed + row.toLong())
        val idx = (mixer.mix(value xor salt) and mask).toInt()
        val c = counters[row * width + idx]
        if (row == 0 || c < min) min = c
    }
    return min
}

/**
 * CountStat-MinStat sketch - a probabilistic frequency estimator over a [depth] x [width] matrix
 * of counters. Each update hashes the value with [depth] independent salts (derived from
 * [seed]) through the [hasher] (default [SplitMix64]) and increments one counter per row;
 * the estimated count for any value is the minimum counter across rows.
 *
 * Estimates are one-sided overestimates: `estimate(x) >= true count(x)` always, with the
 * overestimate bounded by `2 * totalSeen / width` with high probability over the salt
 * choice. Memory is `depth * width` Longs; mergeable element-wise when [depth], [width],
 * and [seed] match.
 *
 * [width] must be a power of two so that the hash maps to an index via masking.
 *
 * **Use cases:** point-frequency estimation for heavy-hitter detection,
 * per-key counters under bounded memory, top-k via paired [SpaceSavingStat].
 *
 * **Memory:** O([depth] · [width]) Longs.
 *
 * **Update:** O([depth]) per observation; [depth] independent atomic adds.
 *
 * **Concurrency:** Striped atomic adds on independent rows. Lock-free and
 * exact under every [Concurrency] level; increments commute, and racing
 * writers on the same value just bump the same cells.
 */
class CountMinSketchStat(
    val depth: Int = 5,
    val width: Int = 1024,
    val seed: Long = -7046029254386353133L, // 0x9e3779b97f4a7c15
    /** Mixer applied to each `value xor rowSalt`; defaults to [SplitMix64]. */
    val hasher: LongHasher = SplitMix64,
    override val concurrency: Concurrency = Concurrency.None,
) : DiscreteStat<CountMinSketchResult> {

    init {
        require(depth > 0) { "depth must be > 0" }
        require(width > 0) { "width must be > 0" }
        require(width and (width - 1) == 0) { "width must be a power of two" }
        // depth * width is the counter-array length and is computed in Int. Unvalidated, a large
        // shape wraps to zero (or worse, to a small positive value) and construction succeeds with
        // an array too short for its own indexing, throwing on the first update instead.
        require(depth.toLong() * width.toLong() <= Int.MAX_VALUE) {
            "depth * width must fit in an Int; $depth * $width overflows"
        }
    }

    private val hasherRef: HasherRef = HasherRef(hasher.name)
    private val mask: Long = (width - 1).toLong()
    private val rowSalts: LongArray = LongArray(depth) { splitmix64(seed + it.toLong()) }
    private val counterCount: Int = depth * width
    private val mode = concurrency.additiveMode()
    private val counters: StreamLongArray = mode.newLongArray(counterCount)
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        // `weight <= 0.0` is false for NaN, and `ceil(NaN).toLong()` is 0, which the coerce below
        // would lift to 1, making a NaN weight a real observation of weight one. Counters are Long
        // and have no NaN to propagate into, so the observation is dropped; see Stat.
        if (weight.isNotPositiveWeight()) return
        // Counters are Long, so a fractional weight has to be rounded. Round *up*: rounding to
        // nearest would discard everything below 0.5, whereas rounding up keeps every observation
        // and leaves `estimate(x) >= true count(x)` intact, since it can only overshoot.
        val w = weight.toCounterStep()
        for (row in 0 until depth) {
            val idx = (hasher.mix(value xor rowSalts[row]) and mask).toInt()
            counters.addSaturating(row * width + idx, w)
        }
        totalSeen.add(1L)
    }

    override fun merge(values: CountMinSketchResult) {
        require(values.depth == depth && values.width == width && values.seed == seed) {
            "Cannot merge CountMinSketchStat with shape " +
                "(${values.depth}, ${values.width}, seed=${values.seed}) " +
                "into ($depth, $width, seed=$seed)"
        }
        // The hasher decides which cell a key lands in, so merging across mixers scatters the
        // incoming counts into cells this sketch will never probe - estimates then come out *below*
        // the true count, breaking the one-sided guarantee silently.
        requireSameHasher("CountMinSketchStat", values.hasher, hasherRef)
        require(values.counters.size == counterCount) {
            "Cannot merge CountMinSketchStat: expected $counterCount counters, got ${values.counters.size}"
        }
        for (i in 0 until counterCount) {
            val incoming = values.counters[i]
            if (incoming != 0L) counters.addSaturating(i, incoming)
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        for (i in 0 until counterCount) counters.store(i, 0L)
        totalSeen.store(0L)
    }

    override fun read(timestampNanos: Long): CountMinSketchResult {
        val snapshot = LongArray(counterCount) { counters.load(it) }
        return CountMinSketchResult(
            depth = depth,
            width = width,
            seed = seed,
            counters = snapshot,
            totalSeen = totalSeen.load(),
            hasher = hasherRef,
        )
    }

    override fun create(concurrency: Concurrency?) = CountMinSketchStat(
        depth,
        width,
        seed,
        hasher,
        concurrency ?: this.concurrency,
    )
}
