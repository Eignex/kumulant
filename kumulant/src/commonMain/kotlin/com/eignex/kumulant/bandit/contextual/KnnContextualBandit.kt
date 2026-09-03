package com.eignex.kumulant.bandit.contextual

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.core.F64VectorStorage
import com.eignex.koblas.forEachStored
import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.ContextualScorable
import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.bandit.argmaxArm
import com.eignex.kumulant.bandit.requireArmIndex
import com.eignex.kumulant.bandit.requireMergeSize
import com.eignex.kumulant.bandit.requireNbrArms
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.preview
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Per-arm snapshot for [KnnContextualBandit]: the retained history of
 * `(context, reward, weight)` triples plus the cumulative observation weight.
 *
 * The arrays are parallel (same length, indexed by sample). Merging is
 * append-then-trim: foreign samples are concatenated to the local history and
 * the oldest entries roll off if the result exceeds `maxHistoryPerArm`.
 */
@Serializable
@SerialName("KnnArmResult")
data class KnnArmResult(
    /** Retained contexts (each a copy of the submitted [F64VectorLike]). */
    val contexts: List<DoubleArray>,
    /** Per-sample rewards, parallel to [contexts]. */
    val rewards: DoubleArray,
    /** Per-sample weights, parallel to [contexts]. */
    val weights: DoubleArray,
    /** Cumulative observation weight folded into this arm. */
    val totalWeight: Double,
) : Result {
    override fun equals(other: Any?): Boolean = other is KnnArmResult &&
        contexts.size == other.contexts.size && contexts.indices.all {
            contexts[it].contentEquals(
                other.contexts[it],
            )
        } &&
        rewards.contentEquals(other.rewards) &&
        weights.contentEquals(other.weights) &&
        totalWeight == other.totalWeight

    override fun hashCode(): Int {
        var h = contexts.fold(0) { acc, a -> 31 * acc + a.contentHashCode() }
        h = 31 * h + rewards.contentHashCode()
        h = 31 * h + weights.contentHashCode()
        h = 31 * h + totalWeight.hashCode()
        return h
    }

    override fun toString(): String = "KnnArmResult(" +
        "contexts=List(${contexts.size}), " +
        "rewards=${rewards.preview()}, " +
        "weights=${weights.preview()}, " +
        "totalWeight=$totalWeight)"
}

/**
 * Non-parametric contextual bandit: each arm keeps a bounded FIFO history of
 * past `(context, reward, weight)` observations and is scored at choose time
 * by the empirical mean reward over the `k` nearest historical contexts,
 * plus an optional UCB-style bonus that decays with the arm's cumulative
 * weight.
 *
 *  - **Distance**: defaults to squared L2 between dense vectors; supply
 *    [distance] for custom metrics (Mahalanobis, cosine, kernelised).
 *  - **History cap**: each arm's reservoir is bounded to [maxHistoryPerArm];
 *    when full, new observations overwrite the oldest.
 *  - **Cold start**: arms with fewer than `k` observations score
 *    `coldStartScore + ucbBonus`, so they are explored before more populous
 *    arms.
 *
 * Per-arm state is a history rather than a sufficient statistic, so the
 * [PerArmBandit] snapshot is a [KnnArmResult] (the bounded reservoir itself)
 * rather than a scalar summary.
 *
 * **Use cases:** contextual problems where reward is a smooth function of
 * context but the functional form is unknown; small-to-medium feature
 * dimensions where exact k-NN is affordable; settings where interpretable
 * "similar past contexts" reasoning is valuable.
 *
 * **Arms:** contextual with caller-defined feature dimension; `nbrArms`
 * fixed at construction. Per-arm reservoir is bounded by [maxHistoryPerArm].
 *
 * **Memory:** O(nbrArms · maxHistoryPerArm · featureSize); each arm owns a
 * fixed-capacity ring of context copies plus parallel primitive reward and
 * weight arrays. A dense slot's buffer is rewritten in place as the ring
 * wraps, so a saturated arm stops allocating altogether. One `3 · k` scan
 * buffer is owned by the bandit and reused across every call.
 *
 * **Choose:** O(nbrArms · maxHistoryPerArm · featureSize), plus O(k) for each
 * candidate that displaces one of the k nearest; a candidate that does not is
 * rejected on a single comparison. Contexts arriving in descending distance
 * order displace on every step and reach O(nbrArms · maxHistoryPerArm · k) for
 * the selection term.
 *
 * **Update:** O(featureSize); copy the context into the ring's next slot and
 * advance the head past the oldest entry when capped.
 *
 * **Randomness:** [random] is held for API uniformity but currently unused;
 * `choose` is deterministic, breaking ties by lowest arm index.
 *
 * **Concurrency:** not thread-safe; the per-arm history rings, the
 * total-weight array, the step counter, and one shared scan buffer are mutated
 * without synchronisation, and an eviction rewrites a live context buffer in
 * place. Serialise `choose` and `update` externally for multi-thread use. The
 * shared buffer also rules out re-entrant scoring, so [distance] must not call
 * back into `choose` or `evaluate`.
 */
class KnnContextualBandit(
    /** Number of arms. */
    override val nbrArms: Int,
    /** Neighbourhood size used for scoring; capped per-arm by the available history. */
    val k: Int = 5,
    /** Maximum observations retained per arm; older entries roll off via FIFO. */
    val maxHistoryPerArm: Int = 1024,
    /** Optimistic value assigned to arms with no history yet; drives initial exploration. */
    val coldStartScore: Double = 1.0,
    /** UCB-style exploration scale on `sqrt(ln(totalSteps) / armWeight)`; `0.0` disables. */
    val exploration: Double = 1.0,
    /** Pairwise distance between context vectors; defaults to squared L2. */
    val distance: (F64VectorLike, F64VectorLike) -> Double = ::squaredL2,
    /** Single source of randomness; used only for tie-breaking, currently deterministic. */
    override val random: Random = Random.Default,
) : ContextualBandit,
    PerArmBandit<KnnArmResult>,
    ContextualScorable {
    init {
        requireNbrArms(nbrArms)
        require(k > 0) { "k must be positive, got $k" }
        require(maxHistoryPerArm > 0) { "maxHistoryPerArm must be positive, got $maxHistoryPerArm" }
        require(exploration >= 0.0) { "exploration must be non-negative, got $exploration" }
    }

    private val histories: Array<ArmHistory> = Array(nbrArms) { ArmHistory(maxHistoryPerArm) }
    private val totalWeights: DoubleArray = DoubleArray(nbrArms)
    private var step: Long = 0L

    // One scan buffer for the life of the bandit: k distances, then the k rewards, then the k
    // weights that go with them. The class already asks callers to serialise `choose` and `update`,
    // so the buffer can be owned outright rather than borrowed per call, which is what makes scoring
    // allocation-free with no workspace to hand. The cost is that scoring is not re-entrant: a
    // [distance] that called back into `choose` or `evaluate` would scan into the buffer its own
    // caller is still reading.
    private val scratch: DoubleArray = DoubleArray(3 * k)

    // Width of the contexts this bandit has been shown, learned from the first one. Without it the
    // only check is the one inside the distance kernel, which does not run until an arm holds k
    // contexts - so a mis-sized context is accepted, and the first call to throw is a later choose
    // reporting a mismatch from a helper rather than from the entry point that took the bad data. A
    // custom distance that skips the check would silently compute against the wrong coordinates.
    private var featureSize: Int = UNKNOWN_FEATURE_SIZE

    private fun requireFeatureSize(size: Int) {
        if (featureSize == UNKNOWN_FEATURE_SIZE) {
            featureSize = size
            return
        }
        require(size == featureSize) { "context size $size != $featureSize" }
    }

    /**
     * Argmax over per-arm [evaluate] scores. Ties broken by lowest index.
     *
     * [workspace] is accepted for interface uniformity and ignored; the scan buffer is owned, so
     * there is no scratch left to borrow.
     */
    @Suppress("UnusedParameter")
    override fun choose(x: F64VectorLike, workspace: Workspace?): Int {
        requireFeatureSize(x.size)
        val bestIdx = argmaxArm(nbrArms) { armIndex -> score(armIndex, x) }
        step++
        return bestIdx
    }

    /**
     * Score arm [armIndex] at context [x]: k-NN mean reward + UCB bonus.
     *
     * [workspace] is accepted for interface uniformity and ignored, as in [choose].
     */
    @Suppress("UnusedParameter")
    override fun evaluate(armIndex: Int, x: F64VectorLike, workspace: Workspace?): Double {
        requireArmIndex(armIndex, nbrArms)
        requireFeatureSize(x.size)
        return score(armIndex, x)
    }

    /** Append `(x, reward, weight)` to arm [armIndex]'s history; oldest entry drops if full. */
    override fun update(armIndex: Int, x: F64VectorLike, reward: Double, weight: Double, workspace: Workspace?) {
        requireArmIndex(armIndex, nbrArms)
        // An inert observation must not consume a history slot; the eviction below would drop real data.
        // A negative weight drops for the same reason rather than downdating: a bounded history has no
        // inverse, so it would append a second phantom entry and leave the arm reporting no evidence
        // while two slots are spent. ReservoirHistogramStat guards the same way. See Stat.
        if (weight.isNotPositiveWeight()) return
        requireFeatureSize(x.size)
        val dropped = histories[armIndex].add(x, reward, weight)
        totalWeights[armIndex] -= dropped
        totalWeights[armIndex] += weight
    }

    /** Live arm history size for [armIndex]. */
    fun historySize(armIndex: Int): Int {
        requireArmIndex(armIndex, nbrArms)
        return histories[armIndex].size
    }

    /** Cumulative observation weight folded into arm [armIndex]. */
    fun armWeight(armIndex: Int): Double {
        requireArmIndex(armIndex, nbrArms)
        return totalWeights[armIndex]
    }

    override fun snapshot(): List<KnnArmResult> = List(nbrArms) { a ->
        val history = histories[a]
        KnnArmResult(
            contexts = List(history.size) { history.contextCopy(it) },
            rewards = DoubleArray(history.size) { history.reward(it) },
            weights = DoubleArray(history.size) { history.weight(it) },
            totalWeight = totalWeights[a],
        )
    }

    override fun merge(other: List<KnnArmResult>, workspace: com.eignex.koblas.Workspace?) {
        requireMergeSize(other.size, nbrArms)
        for (a in 0 until nbrArms) {
            val arm = other[a]
            for (i in arm.contexts.indices) {
                update(a, F64DenseVector.of(arm.contexts[i]), arm.rewards[i], arm.weights[i])
            }
        }
    }

    /** Clear every arm's history. */
    override fun reset() {
        for (a in 0 until nbrArms) {
            histories[a].clear()
            totalWeights[a] = 0.0
        }
        step = 0L
        featureSize = UNKNOWN_FEATURE_SIZE
    }

    /** Spawn a fresh bandit with the same configuration; history resets to empty. */
    override fun create(random: Random): KnnContextualBandit =
        KnnContextualBandit(nbrArms, k, maxHistoryPerArm, coldStartScore, exploration, distance, random)

    private fun ucbBonus(armIndex: Int): Double {
        if (exploration <= 0.0) return 0.0
        val w = totalWeights[armIndex]
        if (w <= 0.0) return exploration * COLD_START_BONUS
        val t = (step + 1L).toDouble()
        return exploration * sqrt(ln(t) / w)
    }

    private fun score(armIndex: Int, x: F64VectorLike): Double {
        if (histories[armIndex].size < k) return coldStartScore + ucbBonus(armIndex)
        return knnMean(armIndex, x) + ucbBonus(armIndex)
    }

    private fun knnMean(armIndex: Int, x: F64VectorLike): Double {
        val history = histories[armIndex]
        // Linear scan holding the k nearest seen so far. For typical maxHistoryPerArm values
        // (<= a few thousand) this is faster than maintaining a KD-tree under reweights.
        // Distances alone are reset: a reward or weight slot is read only where its distance is
        // finite, so whatever the previous arm left in the other two thirds is unreachable.
        for (i in 0 until k) scratch[i] = Double.POSITIVE_INFINITY
        // The slot the next admission displaces, carried rather than rescanned for. Rescanning ran
        // k comparisons for every candidate, admitted or not; carrying it costs one comparison to
        // reject, and past the first k arrivals nearly every candidate is rejected.
        var worst = 0
        var worstDistance = Double.POSITIVE_INFINITY
        // Oldest first, so equal distances resolve to the same neighbour the arrival order picks.
        for (i in 0 until history.size) {
            val d = distance(history.context(i), x)
            // Stated positively so a NaN distance is rejected: negating the test would admit it.
            if (d < worstDistance) {
                scratch[worst] = d
                scratch[k + worst] = history.reward(i)
                scratch[2 * k + worst] = history.weight(i)
                worst = 0
                worstDistance = scratch[0]
                for (j in 1 until k) {
                    if (scratch[j] > worstDistance) {
                        worst = j
                        worstDistance = scratch[j]
                    }
                }
            }
        }
        var sumW = 0.0
        var sumWR = 0.0
        for (i in 0 until k) {
            if (scratch[i] == Double.POSITIVE_INFINITY) continue
            val weight = scratch[2 * k + i]
            sumW += weight
            sumWR += weight * scratch[k + i]
        }
        return if (sumW > 0.0) sumWR / sumW else coldStartScore
    }

    /** Distance-function helpers. */
    companion object {
        private const val COLD_START_BONUS = 1.0

        /** No context has been seen yet, so the next one establishes the expected width. */
        private const val UNKNOWN_FEATURE_SIZE = -1

        /**
         * Squared L2 distance between two vectors of equal size. Sparse-aware:
         * dense/dense walks every index; sparse/dense iterates the sparse side's
         * stored entries and accumulates dense-only contributions via a baseline
         * pass; sparse/sparse iterates the union of stored indices on both sides.
         */
        fun squaredL2(a: F64VectorLike, b: F64VectorLike): Double {
            require(a.size == b.size) { "size mismatch: ${a.size} vs ${b.size}" }
            return when {
                a is F64DenseVector && b is F64DenseVector -> denseSquaredL2(a, b)
                a is F64SparseVector && b is F64SparseVector -> sparseSquaredL2(a, b)
                a is F64SparseVector -> mixedSquaredL2(a, b)
                b is F64SparseVector -> mixedSquaredL2(b, a)
                else -> genericSquaredL2(a, b)
            }
        }

        private fun denseSquaredL2(a: F64DenseVector, b: F64DenseVector): Double {
            var s = 0.0
            for (i in 0 until a.size) {
                val d = a[i] - b[i]
                s += d * d
            }
            return s
        }

        private fun genericSquaredL2(a: F64VectorLike, b: F64VectorLike): Double {
            var s = 0.0
            for (i in 0 until a.size) {
                val d = a[i] - b[i]
                s += d * d
            }
            return s
        }

        private fun mixedSquaredL2(sparse: F64SparseVector, dense: F64VectorLike): Double {
            // Start from the dense side's full squared norm, then correct the sparse-indexed
            // entries to use (sparse_v - dense_v)^2 instead of dense_v^2.
            var s = 0.0
            for (i in 0 until dense.size) {
                val d = dense[i]
                s += d * d
            }
            sparse.forEachStored { i, v ->
                val d = dense[i]
                val replaced = v - d
                s += replaced * replaced - d * d
            }
            return s
        }

        @OptIn(com.eignex.koblas.UnsafeKoblasApi::class)
        private fun sparseSquaredL2(a: F64SparseVector, b: F64SparseVector): Double {
            // Both index arrays are strictly ascending, so one merge walk reaches every coordinate
            // either side stores without a search. Reading the other side positionally would binary
            // search per entry, and testing membership against an IntArray scans it, which makes the
            // pair cost the product of the two nonzero counts rather than their sum.
            val ai = a.indices
            val bi = b.indices
            val av = a.values
            val bv = b.values
            var s = 0.0
            var p = 0
            var q = 0
            while (p < ai.size && q < bi.size) {
                val ip = ai[p]
                val iq = bi[q]
                when {
                    ip == iq -> {
                        val d = av[p] - bv[q]
                        s += d * d
                        p++
                        q++
                    }

                    ip < iq -> {
                        s += av[p] * av[p]
                        p++
                    }

                    else -> {
                        s += bv[q] * bv[q]
                        q++
                    }
                }
            }
            // A coordinate stored on one side only differs from the other side's implicit zero, so
            // the tails contribute their own squares. A stored zero contributes zero, exactly once.
            while (p < ai.size) {
                s += av[p] * av[p]
                p++
            }
            while (q < bi.size) {
                s += bv[q] * bv[q]
                q++
            }
            return s
        }
    }
}

// Fixed-capacity FIFO of (context, reward, weight) samples for one arm, addressed by age with 0 the
// oldest live sample. Rewards and weights sit in DoubleArrays so a sample costs no boxing, and
// eviction advances the head onto the slot it overwrites, so a full arm admits a sample in O(width)
// rather than shifting the whole history.
//
// Contexts stay in koblas storages rather than one flat buffer because koblas has no dense vector
// over an (array, offset, length) slice: a window type declared here would be a third implementation
// of F64VectorLike, which drops the scan out of squaredL2's dense/dense branch onto its generic one
// and costs more per choose than the flat layout saves per update.
private class ArmHistory(private val capacity: Int) {
    var size: Int = 0
        private set

    private val rewards = DoubleArray(capacity)
    private val weights = DoubleArray(capacity)
    private val contexts = arrayOfNulls<F64VectorStorage>(capacity)

    private var head = 0

    fun reward(age: Int): Double = rewards[slotOf(age)]

    fun weight(age: Int): Double = weights[slotOf(age)]

    /** The stored context at [age], oldest first. */
    fun context(age: Int): F64VectorLike = contexts[slotOf(age)]!!

    /** The stored context at [age], densified into an array that outlives the slot. */
    fun contextCopy(age: Int): DoubleArray = contexts[slotOf(age)]!!.toDoubleArray()

    /** Admits `(x, reward, weight)` and returns the weight of the sample it evicted, `0.0` if none. */
    fun add(x: F64VectorLike, reward: Double, weight: Double): Double {
        val slot: Int
        var dropped = 0.0
        if (size == capacity) {
            dropped = weights[head]
            slot = head
            head = if (head + 1 == capacity) 0 else head + 1
        } else {
            slot = slotOf(size)
            size++
        }
        rewards[slot] = reward
        weights[slot] = weight
        contexts[slot] = retain(contexts[slot], x)
        return dropped
    }

    fun clear() {
        size = 0
        head = 0
        contexts.fill(null)
    }

    private fun slotOf(age: Int): Int {
        val raw = head + age
        return if (raw >= capacity) raw - capacity else raw
    }
}

// The copy of x an arm keeps, reusing the dense buffer already in the slot when the widths agree so a
// saturated arm evicts without allocating. A sparse sample always takes a fresh copy: its stored
// length tracks the sample rather than the feature width, and the caller keeps arrays it may mutate.
private fun retain(slot: F64VectorStorage?, x: F64VectorLike): F64VectorStorage {
    if (x is F64SparseVector) return F64SparseVector.wrap(x.size, x.copyIndices(), x.values.copyOf())
    if (slot is F64DenseVector && slot.size == x.size) {
        if (x is F64DenseVector) {
            x.data.copyInto(slot.data)
        } else {
            for (i in 0 until x.size) slot.data[i] = x[i]
        }
        return slot
    }
    return F64DenseVector.wrap(x.toDoubleArray())
}
