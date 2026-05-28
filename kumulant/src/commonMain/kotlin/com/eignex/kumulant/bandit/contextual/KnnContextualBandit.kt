package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.ContextualScorable
import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.SparseVector
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.math.forEachStored
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
    /** Retained contexts (each a copy of the submitted [VectorView]). */
    val contexts: List<DoubleArray>,
    /** Per-sample rewards, parallel to [contexts]. */
    val rewards: DoubleArray,
    /** Per-sample weights, parallel to [contexts]. */
    val weights: DoubleArray,
    /** Cumulative observation weight folded into this arm. */
    val totalWeight: Double,
) : Result

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
 * **Memory:** O(nbrArms · maxHistoryPerArm · featureSize); bounded
 * per-arm history of context copies plus parallel reward/weight arrays.
 *
 * **Choose:** O(nbrArms · maxHistoryPerArm · (featureSize + k)); linear
 * scan over each arm's history with a bounded top-k heap.
 *
 * **Update:** O(featureSize); append context copy and roll the oldest
 * entry off when capped.
 *
 * **Randomness:** [random] is held for API uniformity but currently unused;
 * `choose` is deterministic, breaking ties by lowest arm index.
 *
 * **Concurrency:** not thread-safe; per-arm history mutable lists, the
 * total-weight array, and the step counter are mutated without
 * synchronisation. Serialise `choose` and `update` externally for
 * multi-thread use.
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
    val distance: (VectorView, VectorView) -> Double = ::squaredL2,
    /** Single source of randomness; used only for tie-breaking, currently deterministic. */
    override val random: Random = Random.Default,
) : ContextualBandit,
    PerArmBandit<KnnArmResult>,
    ContextualScorable {
    init {
        require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
        require(k > 0) { "k must be positive, got $k" }
        require(maxHistoryPerArm > 0) { "maxHistoryPerArm must be positive, got $maxHistoryPerArm" }
        require(exploration >= 0.0) { "exploration must be non-negative, got $exploration" }
    }

    private val historyContexts: Array<MutableList<VectorView>> = Array(nbrArms) { mutableListOf() }
    private val historyRewards: Array<MutableList<Double>> = Array(nbrArms) { mutableListOf() }
    private val historyWeights: Array<MutableList<Double>> = Array(nbrArms) { mutableListOf() }
    private val totalWeights: DoubleArray = DoubleArray(nbrArms)
    private var step: Long = 0L

    /** Argmax over per-arm [evaluate] scores. Ties broken by lowest index. */
    override fun choose(x: VectorView): Int {
        var bestIdx = 0
        var bestScore = Double.NEGATIVE_INFINITY
        for (a in 0 until nbrArms) {
            val s = evaluate(a, x)
            if (s > bestScore) {
                bestScore = s
                bestIdx = a
            }
        }
        step++
        return bestIdx
    }

    /** Score arm [armIndex] at context [x]: k-NN mean reward + UCB bonus. */
    override fun evaluate(armIndex: Int, x: VectorView): Double {
        require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
        val ctx = historyContexts[armIndex]
        if (ctx.size < k) return coldStartScore + ucbBonus(armIndex)
        val mean = knnMean(armIndex, x)
        return mean + ucbBonus(armIndex)
    }

    /** Append `(x, reward, weight)` to arm [armIndex]'s history; oldest entry drops if full. */
    override fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double) {
        require(armIndex in 0 until nbrArms) { "armIndex out of bounds: $armIndex" }
        val ctxs = historyContexts[armIndex]
        val rs = historyRewards[armIndex]
        val ws = historyWeights[armIndex]
        if (ctxs.size >= maxHistoryPerArm) {
            val dropped = ws.removeAt(0)
            ctxs.removeAt(0)
            rs.removeAt(0)
            totalWeights[armIndex] -= dropped
        }
        ctxs += copyOf(x)
        rs += reward
        ws += weight
        totalWeights[armIndex] += weight
    }

    /** Live arm history size for [armIndex]. */
    fun historySize(armIndex: Int): Int = historyContexts[armIndex].size

    /** Cumulative observation weight folded into arm [armIndex]. */
    fun armWeight(armIndex: Int): Double = totalWeights[armIndex]

    override fun snapshot(): List<KnnArmResult> = List(nbrArms) { a ->
        KnnArmResult(
            contexts = historyContexts[a].map { it.toDoubleArray() },
            rewards = historyRewards[a].toDoubleArray(),
            weights = historyWeights[a].toDoubleArray(),
            totalWeight = totalWeights[a],
        )
    }

    override fun merge(other: List<KnnArmResult>) {
        require(other.size == nbrArms) {
            "merge: other.size=${other.size} does not match nbrArms=$nbrArms"
        }
        for (a in 0 until nbrArms) {
            val arm = other[a]
            for (i in arm.contexts.indices) {
                update(a, DenseVector.of(arm.contexts[i]), arm.rewards[i], arm.weights[i])
            }
        }
    }

    /** Clear every arm's history. */
    override fun reset() {
        for (a in 0 until nbrArms) {
            historyContexts[a].clear()
            historyRewards[a].clear()
            historyWeights[a].clear()
            totalWeights[a] = 0.0
        }
        step = 0L
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

    private fun knnMean(armIndex: Int, x: VectorView): Double {
        val ctxs = historyContexts[armIndex]
        val rs = historyRewards[armIndex]
        val ws = historyWeights[armIndex]
        // Linear scan, bounded heap of size k. For typical maxHistoryPerArm values
        // (<= a few thousand) this is faster than maintaining a KD-tree under reweights.
        val topD = DoubleArray(k) { Double.POSITIVE_INFINITY }
        val topR = DoubleArray(k)
        val topW = DoubleArray(k)
        for (i in ctxs.indices) {
            val d = distance(ctxs[i], x)
            // Find insertion point: index of the max in topD.
            var worst = 0
            for (j in 1 until k) if (topD[j] > topD[worst]) worst = j
            if (d < topD[worst]) {
                topD[worst] = d
                topR[worst] = rs[i]
                topW[worst] = ws[i]
            }
        }
        var sumW = 0.0
        var sumWR = 0.0
        for (i in 0 until k) {
            if (topD[i] == Double.POSITIVE_INFINITY) continue
            sumW += topW[i]
            sumWR += topW[i] * topR[i]
        }
        return if (sumW > 0.0) sumWR / sumW else coldStartScore
    }

    /** Distance-function helpers. */
    companion object {
        private const val COLD_START_BONUS = 1.0

        /**
         * Squared L2 distance between two vectors of equal size. Sparse-aware:
         * dense/dense walks every index; sparse/dense iterates the sparse side's
         * stored entries and accumulates dense-only contributions via a baseline
         * pass; sparse/sparse iterates the union of stored indices on both sides.
         */
        fun squaredL2(a: VectorView, b: VectorView): Double {
            require(a.size == b.size) { "size mismatch: ${a.size} vs ${b.size}" }
            return when {
                a is DenseVector && b is DenseVector -> denseSquaredL2(a, b)
                a is SparseVector && b is SparseVector -> sparseSquaredL2(a, b)
                a is SparseVector -> mixedSquaredL2(a, b)
                else -> mixedSquaredL2(b as SparseVector, a)
            }
        }

        private fun denseSquaredL2(a: DenseVector, b: DenseVector): Double {
            var s = 0.0
            for (i in 0 until a.size) {
                val d = a[i] - b[i]
                s += d * d
            }
            return s
        }

        private fun mixedSquaredL2(sparse: SparseVector, dense: VectorView): Double {
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

        private fun sparseSquaredL2(a: SparseVector, b: SparseVector): Double {
            var s = 0.0
            a.forEachStored { i, v ->
                val d = v - b[i]
                s += d * d
            }
            b.forEachStored { i, v ->
                if (a[i] == 0.0) s += v * v
            }
            return s
        }
    }

    private fun copyOf(x: VectorView): VectorView = when (x) {
        is DenseVector -> DenseVector.of(x.toDoubleArray())

        is SparseVector -> {
            val keepIdx = IntArray(x.size)
            val keepVal = DoubleArray(x.size)
            var n = 0
            x.forEachStored { i, v ->
                keepIdx[n] = i
                keepVal[n] = v
                n++
            }
            SparseVector.of(x.size, keepIdx.copyOf(n), keepVal.copyOf(n))
        }
    }
}
