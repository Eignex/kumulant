package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.math.CounterRandom
import com.eignex.kumulant.math.nextPoissonOne
import kotlin.concurrent.Volatile

// Tree population shared by both random forests: Oza & Russell online bagging over trees whose
// seeds come from one stream, so a reset forest regrows exactly what a fresh one would.
internal class BaggedTrees<T>(
    private val nbrTrees: Int,
    private val bagging: Boolean,
    concurrency: Concurrency,
    randomSeed: Int,
    private val newTree: (seed: Int) -> T,
) {
    init {
        require(nbrTrees > 0) { "nbrTrees must be positive, got $nbrTrees" }
    }

    // The bagging stream is drawn from the seed stream before any tree seed; keep that order.
    private val seedRng = CounterRandom(randomSeed.toLong(), concurrency)
    private val baggingRng = CounterRandom(seedRng.childSeed(), concurrency)

    // Volatile publication makes reset replacements visible to concurrent updates.
    @Volatile
    var trees: List<T> = grow()
        private set

    private fun grow(): List<T> = List(nbrTrees) { newTree(seedRng.nextInt()) }

    /**
     * Whether an update of [weight] reaches the trees. Bagging draws independent Poisson
     * multiplicities, so a retraction cannot recover the weight it inserted; it is dropped before
     * any draw so the sequence stays intact for later observations.
     */
    fun accepts(weight: Double): Boolean = if (bagging) !weight.isNotPositiveWeight() else !weight.isInertWeight()

    /** Multiplicity of the current update for the next tree; always 1 without bagging. */
    fun multiplicity(): Int = if (bagging) baggingRng.nextPoissonOne() else 1

    /** Restores empty trees and restarts the seed streams for reproducible growth. */
    fun reset() {
        seedRng.reset()
        baggingRng.reset()
        trees = grow()
    }

    /** A seed for a replica forest, distinct on every call. */
    fun childSeed(): Int = seedRng.childSeed().toInt()
}
