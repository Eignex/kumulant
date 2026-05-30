@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stream.Mutex
import com.eignex.kumulant.stream.NoopMutex
import com.eignex.kumulant.stream.PlatformMutex
import kotlin.concurrent.Volatile
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Classification mirror of [RegressionTree]: online VFDT decision tree where each leaf carries
 * a per-class count accumulator and audit leaves track class counts per candidate
 * split. Splits fire when a candidate clears the Hoeffding bound on the configured
 * [ClassificationSplitMetric] (Gini or information gain).
 *
 * Concurrency model matches [RegressionTree]: lock-free leaf updates, single split-conversion
 * lock fired only at split-decision time.
 */
class ClassificationTree(
    private val numClasses: Int,
    private val splitCandidates: List<SerializableSplit>,
    private val config: ClassificationTreeConfig = ClassificationTreeConfig(),
    private val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: () -> SeriesStat<ClassCountsResult> = { ClassCountsStat(numClasses, concurrency) },
    randomSeed: Int = 0,
) {
    init {
        require(numClasses >= 2) { "numClasses must be >= 2; got $numClasses" }
    }

    private val random = Random(randomSeed)
    private val canGrow: Boolean = splitCandidates.isNotEmpty()
    private val splitLock: Mutex = if (concurrency == Concurrency.None) NoopMutex else PlatformMutex()

    private val nbrNodes: AtomicInt = AtomicInt(1)

    @Volatile
    private var root: ClassificationNode = newLeaf(depth = 0)

    /** Walk to the leaf [row] resolves to. */
    fun findLeaf(row: VectorView): ClassificationLeafNode = root.findLeaf(row)

    /** Live root node, for snapshotting. */
    fun rootNode(): ClassificationNode = root

    /** Argmax class at the leaf [row] resolves to. */
    fun predict(row: VectorView): Int = findLeaf(row).arm.read(0L).predict()

    /** Probabilities at the leaf [row] resolves to. */
    fun probabilities(row: VectorView): DoubleArray = findLeaf(row).arm.read(0L).probabilities()

    /** Number of internal + leaf nodes currently in the tree. */
    val nodeCount: Int get() = nbrNodes.load()

    /** Fold an observation `(row, classLabel)` into the tree, possibly growing it. */
    fun update(row: VectorView, classLabel: Int, weight: Double = 1.0) {
        if (classLabel !in 0 until numClasses) return
        val current = root
        val next = updateNode(current, row, classLabel, weight, depth = 0)
        if (next !== current) root = next
    }

    /** Aggregate class-count snapshot at the root, walking leaves and split carryovers. */
    fun rootSnapshot(): ClassCountsResult = root.subtreeAggregate()

    /** Reset to a single fresh leaf. */
    fun reset() {
        splitLock.withLock {
            nbrNodes.store(1)
            root = newLeaf(depth = 0)
        }
    }

    /** Render the tree as nested if-else text. */
    fun prettyPrint(indent: String = ""): String = buildString { prettyPrintTo(this, root, indent) }

    /** Structurally merge [other] into this tree; [other] is consumed. */
    fun merge(other: ClassificationTree) {
        splitLock.withLock {
            root = mergeNodes(root, other.root)
            nbrNodes.store(countNodes(root))
        }
    }

    /** Snapshot merge: same rules as [merge] but the other side is an immutable result. */
    fun mergeSnapshot(other: TreeClassificationNodeResult) {
        splitLock.withLock {
            root = mergeNodeWithResult(root, other)
            nbrNodes.store(countNodes(root))
        }
    }

    private fun mergeNodes(a: ClassificationNode, b: ClassificationNode): ClassificationNode {
        if (a is ClassificationSplitNode && b is ClassificationSplitNode && a.split == b.split) {
            a.pos = mergeNodes(a.pos, b.pos)
            a.neg = mergeNodes(a.neg, b.neg)
            val bCarry = b.carryover
            if (bCarry != null) foldIntoCarryover(a, bCarry.read(0L))
            return a
        }
        if (a is ClassificationLeafNode && b is ClassificationLeafNode) {
            a.arm.merge(b.arm.read(0L))
            return a
        }
        if (a is ClassificationLeafNode && b is ClassificationSplitNode) {
            foldIntoCarryover(b, a.arm.read(0L))
            return b
        }
        foldIntoCarryover(a as ClassificationSplitNode, b.subtreeAggregate())
        return a
    }

    private fun mergeNodeWithResult(a: ClassificationNode, b: TreeClassificationNodeResult): ClassificationNode {
        if (a is ClassificationSplitNode && b is TreeClassificationSplitResult && a.split == b.split) {
            a.pos = mergeNodeWithResult(a.pos, b.pos)
            a.neg = mergeNodeWithResult(a.neg, b.neg)
            val childSum = mergeCC(b.pos.value, b.neg.value)
            val residual = subtractCC(b.value, childSum)
            if (residual.totalWeights > 0.0) foldIntoCarryover(a, residual)
            return a
        }
        if (a is ClassificationLeafNode && b is TreeClassificationLeafResult) {
            a.arm.merge(b.value)
            return a
        }
        if (a is ClassificationLeafNode && b is TreeClassificationSplitResult) {
            val cloned = cloneFromResult(b, depth = 0) as ClassificationSplitNode
            foldIntoCarryover(cloned, a.arm.read(0L))
            return cloned
        }
        foldIntoCarryover(a as ClassificationSplitNode, b.value)
        return a
    }

    private fun foldIntoCarryover(node: ClassificationSplitNode, value: ClassCountsResult) {
        if (value.totalWeights <= 0.0) return
        val existing = node.carryover
        if (existing != null) {
            existing.merge(value)
        } else {
            val arm = leafArmFactory()
            arm.merge(value)
            node.carryover = arm
        }
    }

    private fun cloneFromResult(node: TreeClassificationNodeResult, depth: Int): ClassificationNode {
        nbrNodes.addAndFetch(1)
        return when (node) {
            is TreeClassificationLeafResult -> {
                val arm = leafArmFactory()
                arm.merge(node.value)
                ClassificationTerminalLeaf(arm)
            }

            is TreeClassificationSplitResult -> {
                val pos = cloneFromResult(node.pos, depth + 1)
                val neg = cloneFromResult(node.neg, depth + 1)
                val childSum = mergeCC(node.pos.value, node.neg.value)
                val residual = subtractCC(node.value, childSum)
                val carry = if (residual.totalWeights > 0.0) leafArmFactory().also { it.merge(residual) } else null
                ClassificationSplitNode(split = node.split, pos = pos, neg = neg, carryover = carry)
            }
        }
    }

    private fun countNodes(node: ClassificationNode): Int = when (node) {
        is ClassificationSplitNode -> 1 + countNodes(node.pos) + countNodes(node.neg)
        is ClassificationLeafNode -> 1
    }

    private fun prettyPrintTo(sb: StringBuilder, node: ClassificationNode, indent: String) {
        when (node) {
            is ClassificationSplitNode -> {
                sb.append(indent).append("if (").append(node.split.toString()).append(") {\n")
                prettyPrintTo(sb, node.pos, "$indent  ")
                sb.append(indent).append("} else {\n")
                prettyPrintTo(sb, node.neg, "$indent  ")
                sb.append(indent).append("}\n")
            }

            is ClassificationLeafNode -> {
                val r = node.arm.read(0L)
                sb.append(indent).append("leaf counts=").append(r.counts.toList()).append('\n')
            }
        }
    }

    private fun newLeaf(depth: Int): ClassificationLeafNode {
        if (depth >= config.maxDepth || nbrNodes.load() + 1 > config.maxNodes || !canGrow) {
            return ClassificationTerminalLeaf(leafArmFactory())
        }
        val subset = pickCandidates()
        return ClassificationAuditLeaf(
            arm = leafArmFactory(),
            candidates = subset,
            pos = List(subset.size) { leafArmFactory() },
            neg = List(subset.size) { leafArmFactory() },
        )
    }

    private fun pickCandidates(): List<SerializableSplit> {
        val k = config.mtry ?: return splitCandidates
        if (k >= splitCandidates.size) return splitCandidates
        return splitCandidates.shuffled(random).take(k)
    }

    private fun updateNode(
        node: ClassificationNode,
        row: VectorView,
        classLabel: Int,
        weight: Double,
        depth: Int,
    ): ClassificationNode = when (node) {
        is ClassificationSplitNode -> {
            if (node.split.direction(row)) {
                val child = node.pos
                val next = updateNode(child, row, classLabel, weight, depth + 1)
                if (next !== child) node.pos = next
            } else {
                val child = node.neg
                val next = updateNode(child, row, classLabel, weight, depth + 1)
                if (next !== child) node.neg = next
            }
            node
        }

        is ClassificationTerminalLeaf -> {
            node.arm.update(classLabel.toDouble(), 0L, weight)
            node
        }

        is ClassificationAuditLeaf -> updateAuditLeaf(node, row, classLabel, weight, depth)
    }

    private fun updateAuditLeaf(
        leaf: ClassificationAuditLeaf,
        row: VectorView,
        classLabel: Int,
        weight: Double,
        depth: Int,
    ): ClassificationNode {
        val y = classLabel.toDouble()
        leaf.arm.update(y, 0L, weight)
        for ((i, split) in leaf.candidates.withIndex()) {
            if (split.direction(row)) {
                leaf.pos[i].update(y, 0L, weight)
            } else {
                leaf.neg[i].update(y, 0L, weight)
            }
        }
        val ticks = leaf.observationsSinceLastCheck.addAndFetch(1L)
        if (ticks < config.splitPeriod) return leaf

        return splitLock.withLock {
            if (leaf.observationsSinceLastCheck.load() < config.splitPeriod) return@withLock leaf
            leaf.observationsSinceLastCheck.store(0L)

            val total = leaf.arm.read(0L)
            if (total.totalWeights < config.minSamplesSplit) return@withLock leaf
            val pos = leaf.pos.map { it.read(0L) }
            val neg = leaf.neg.map { it.read(0L) }
            val ranked = config.metric.rank(total, pos, neg, config.minSamplesSplit, config.minSamplesLeaf)
            if (ranked.bestIndex < 0 || ranked.top1 <= 0.0) return@withLock leaf

            val eps = hoeffdingBound(config.delta, total.totalWeights, depth, config.deltaDecay)
            val passesHoeffding = ranked.top1 - ranked.top2 > eps
            val passesTau = eps < config.tau
            if (!passesHoeffding && !passesTau) return@withLock leaf

            nbrNodes.addAndFetch(2)
            ClassificationSplitNode(
                split = leaf.candidates[ranked.bestIndex],
                pos = newLeaf(depth + 1),
                neg = newLeaf(depth + 1),
                carryover = leaf.arm,
            )
        }
    }

    private fun hoeffdingBound(delta: Double, n: Double, depth: Int, decay: Double): Double {
        if (n <= 0.0) return Double.POSITIVE_INFINITY
        val adjusted = delta * decay.pow(depth.toDouble())
        return sqrt(-ln(adjusted) / (2.0 * n))
    }
}
