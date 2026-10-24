package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.Vector
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Weighted training observation for batch regression.
 *
 * @property features Numeric feature vector; copied when fitting.
 * @property response Finite scalar response.
 * @property weight Nonnegative finite observation weight; zero excludes the row.
 */
data class RegressionObservation(val features: Vector, val response: Double, val weight: Double = 1.0) {
    init {
        require(response.isFinite()) { "response must be finite" }
        require(weight >= 0.0 && weight.isFinite()) { "weight must be nonnegative and finite" }
    }
}

/** Threshold selection for each feature inspected by a batch regression tree. */
enum class BatchSplitStrategy {
    /** Scan all distinct observed boundaries for maximum variance reduction. */
    Best,

    /** Sample thresholds uniformly across the observed feature range. */
    Random,
}

/** Configuration for a batch CART regression forest. */
data class BatchForestConfig(
    /** Number of independently randomized CART trees. */
    val nbrTrees: Int = 10,
    /** Maximum root-to-leaf depth; zero produces constant trees. */
    val maxDepth: Int = 20,
    /** Maximum nodes per tree, including leaves. */
    val maxNodes: Int = 10_000,
    /** Minimum statistical weight on each side of a split. */
    val minSamplesLeaf: Double = 1.0,
    /** Features inspected per node; null selects ceil(sqrt(featureSize)). Invalid subsets search further. */
    val mtry: Int? = null,
    /** Resample rows with replacement independently for each tree. */
    val bootstrap: Boolean = true,
    /** Minimum reduction in weighted population variance required to split. */
    val minVarianceReduction: Double = 0.0,
    /** Greedy or randomized threshold search. */
    val splitStrategy: BatchSplitStrategy = BatchSplitStrategy.Best,
    /** Threshold draws per inspected feature under [BatchSplitStrategy.Random]. */
    val randomThresholds: Int = 1,
) {
    init {
        require(nbrTrees > 0 && maxDepth >= 0 && maxNodes > 0) { "invalid tree count or size limits" }
        require(minSamplesLeaf > 0.0 && minSamplesLeaf.isFinite()) { "minSamplesLeaf must be positive and finite" }
        require(mtry == null || mtry > 0) { "mtry must be positive" }
        require(minVarianceReduction >= 0.0 && minVarianceReduction.isFinite()) { "invalid variance reduction" }
        require(randomThresholds > 0) { "randomThresholds must be positive" }
    }
}

/**
 * Batch Breiman-style random forest, rebuilding CART trees from all supplied observations.
 * Each node selects a random feature subset and searches thresholds for weighted variance
 * reduction. [BatchSplitStrategy.Best] scans observed boundaries; [BatchSplitStrategy.Random]
 * samples within each feature's observed range. Bootstrap sampling selects rows
 * uniformly with replacement; observation weights scale their statistical contributions.
 *
 * Numeric thresholds preserve continuous input values. The
 * returned [ForestRegressionResult] shares the prediction and serialization API of online
 * forests. Inputs are copied during fitting and subsequent fits produce independent models.
 *
 * **Use cases:** history-based optimization and supervised regression with full retraining.
 *
 * **Memory:** O(n * featureSize + nbrTrees * maxNodes), plus per-node sorting buffers.
 *
 * **Fit:** O(nbrTrees * featureSize * n * (log(n) + randomThresholds) * maxDepth) as a conservative bound.
 *
 * **Concurrency:** No mutable fitter state. Calls are independent when inputs and RNGs are independent.
 *
 * @property featureSize Expected number of numeric coordinates.
 * @property config CART and ensemble configuration.
 */
class BatchRandomForest(val featureSize: Int, val config: BatchForestConfig = BatchForestConfig()) {
    init {
        require(featureSize >= 0) { "featureSize must be nonnegative" }
    }

    /** Fit an independent forest; an empty dataset produces empty constant trees. */
    fun fit(observations: List<RegressionObservation>, random: Random = Random.Default): ForestRegressionResult {
        val rows = observations.map { observation ->
            require(observation.features.size == featureSize) { "feature dimension mismatch" }
            val values = observation.features.toDoubleArray()
            require(values.all { it.isFinite() }) { "features must be finite" }
            TrainingRow(values, observation.response, observation.weight)
        }.filter { it.weight > 0.0 }
        return ForestRegressionResult(
            List(config.nbrTrees) {
                val training = if (config.bootstrap && rows.isNotEmpty()) bootstrap(rows, random) else rows
                TreeRegressionResult(BatchTreeBuilder(config, featureSize, random).grow(training, 0))
            },
        )
    }

    private fun bootstrap(rows: List<TrainingRow>, random: Random): List<TrainingRow> {
        val counts = IntArray(rows.size)
        repeat(rows.size) { counts[random.nextInt(rows.size)]++ }
        return rows.indices.filter { counts[it] > 0 }.map { index ->
            rows[index].copy(weight = rows[index].weight * counts[index])
        }
    }
}

private data class TrainingRow(val features: DoubleArray, val response: Double, val weight: Double)

private data class BatchSplit(val feature: Int, val threshold: Double, val gain: Double)

private class BatchTreeBuilder(
    private val config: BatchForestConfig,
    private val featureSize: Int,
    private val random: Random,
) {
    private var nodes = 1

    fun grow(rows: List<TrainingRow>, depth: Int): TreeNodeResult {
        val summary = summarize(rows)
        if (rows.size < 2 || depth >= config.maxDepth || nodes + 2 > config.maxNodes ||
            summary.totalWeights < 2.0 * config.minSamplesLeaf || summary.variance <= 0.0
        ) {
            return TreeLeafResult(summary)
        }
        val split = bestSplit(rows, summary) ?: return TreeLeafResult(summary)
        val (pos, neg) = rows.partition { it.features[split.feature] <= split.threshold }
        nodes += 2
        return TreeSplitResult(
            ThresholdSplit(split.feature, split.threshold),
            grow(pos, depth + 1),
            grow(neg, depth + 1),
            summary,
        )
    }

    private fun bestSplit(rows: List<TrainingRow>, summary: WeightedVarianceResult): BatchSplit? {
        val count = config.mtry ?: kotlin.math.ceil(sqrt(featureSize.toDouble())).toInt()
        var best: BatchSplit? = null
        for ((visited, feature) in (0 until featureSize).shuffled(random).withIndex()) {
            val split = when (config.splitStrategy) {
                BatchSplitStrategy.Best -> axisSplit(rows, feature, summary)
                BatchSplitStrategy.Random -> randomSplit(rows, feature, summary)
            }
            if (split != null && split.gain > (best?.gain ?: Double.NEGATIVE_INFINITY)) best = split
            if (visited + 1 >= count && best != null) break
        }
        return best
    }

    private fun randomSplit(rows: List<TrainingRow>, feature: Int, summary: WeightedVarianceResult): BatchSplit? {
        val lo = rows.minOf { it.features[feature] }
        val hi = rows.maxOf { it.features[feature] }
        if (lo == hi) return null
        var best: BatchSplit? = null
        repeat(config.randomThresholds) {
            val unit = random.nextDouble()
            val threshold = ((1.0 - unit) * lo + unit * hi).let { if (it < hi) it else lo }
            val (leftRows, rightRows) = rows.partition { it.features[feature] <= threshold }
            if (leftRows.isEmpty() || rightRows.isEmpty()) return@repeat
            val left = summarize(leftRows)
            val right = summarize(rightRows)
            if (left.totalWeights < config.minSamplesLeaf || right.totalWeights < config.minSamplesLeaf) return@repeat
            val residual =
                (left.variance * left.totalWeights + right.variance * right.totalWeights) / summary.totalWeights
            val gain = summary.variance - residual
            if (gain >= config.minVarianceReduction && gain > (best?.gain ?: Double.NEGATIVE_INFINITY)) {
                best = BatchSplit(feature, threshold, gain)
            }
        }
        return best
    }

    private fun axisSplit(rows: List<TrainingRow>, feature: Int, summary: WeightedVarianceResult): BatchSplit? {
        val sorted = rows.sortedBy { it.features[feature] }
        val rightStat = VarianceStat()
        val suffix = arrayOfNulls<WeightedVarianceResult>(sorted.size)
        for (i in sorted.indices.reversed()) {
            rightStat.update(sorted[i].response, 0L, sorted[i].weight)
            suffix[i] = rightStat.read(0L)
        }
        val leftStat = VarianceStat()
        var best: BatchSplit? = null
        for (i in 0 until sorted.lastIndex) {
            leftStat.update(sorted[i].response, 0L, sorted[i].weight)
            val lo = sorted[i].features[feature]
            val hi = sorted[i + 1].features[feature]
            if (lo == hi) continue
            val left = leftStat.read(0L)
            val right = requireNotNull(suffix[i + 1])
            if (left.totalWeights < config.minSamplesLeaf || right.totalWeights < config.minSamplesLeaf) continue
            val residual =
                (left.variance * left.totalWeights + right.variance * right.totalWeights) / summary.totalWeights
            val gain = summary.variance - residual
            if (gain < config.minVarianceReduction || gain <= (best?.gain ?: Double.NEGATIVE_INFINITY)) continue
            val midpoint = lo / 2.0 + hi / 2.0
            best = BatchSplit(feature, if (midpoint >= lo && midpoint < hi) midpoint else lo, gain)
        }
        return best
    }

    private fun summarize(rows: List<TrainingRow>): WeightedVarianceResult {
        val stat = VarianceStat()
        for (row in rows) stat.update(row.response, 0L, row.weight)
        return stat.read(0L)
    }
}
