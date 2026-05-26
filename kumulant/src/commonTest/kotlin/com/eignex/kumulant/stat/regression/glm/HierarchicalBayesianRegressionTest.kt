package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.math.DenseVector
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HierarchicalBayesianRegressionTest {

    @Test
    fun `rejects non-positive featureSize`() {
        assertFailsWith<IllegalArgumentException> {
            HierarchicalBayesianRegression(featureSize = 0)
        }
    }

    @Test
    fun `initial populationPrior is isotropic when no overrides supplied`() {
        val pop = HierarchicalBayesianRegression(featureSize = 3, initialPriorVariance = 2.0)
        val prior = pop.populationPrior
        assertEquals(3, prior.mean.size)
        // Default mean is zero
        for (i in 0 until 3) assertEquals(0.0, prior.mean[i])
        // Diagonal = 2.0
        for (i in 0 until 3) assertEquals(2.0, prior.covariance[i, i])
        // Off-diagonals = 0
        for (i in 0 until 3) {
            for (j in 0 until 3) {
                if (i != j) assertEquals(0.0, prior.covariance[i, j])
            }
        }
        assertEquals(0, prior.instanceCount)
    }

    @Test
    fun `createInstance seeds from current populationPrior`() {
        val pop = HierarchicalBayesianRegression(featureSize = 2, initialPriorVariance = 5.0)
        val inst = pop.createInstance()
        val snap = inst.read()
        // Fresh instance: weights start at prior mean (zeros)
        assertEquals(0.0, snap.weights[0])
        assertEquals(0.0, snap.weights[1])
        // And totalWeights is 0 (no observations folded in yet)
        assertEquals(0.0, snap.totalWeights)
    }

    @Test
    fun `instanceCount tracks createInstance and untrack`() {
        val pop = HierarchicalBayesianRegression(featureSize = 2)
        assertEquals(0, pop.instanceCount)
        val a = pop.createInstance()
        val b = pop.createInstance()
        assertEquals(2, pop.instanceCount)
        pop.untrack(a)
        assertEquals(1, pop.instanceCount)
        @Suppress("UNUSED_VARIABLE")
        val unused = b
    }

    @Test
    fun `refit is a no-op with no tracked instances`() {
        val pop = HierarchicalBayesianRegression(featureSize = 2)
        val before = pop.populationPrior
        pop.refit()
        // populationPrior is still the same initial isotropic prior
        assertEquals(before.mean[0], pop.populationPrior.mean[0])
        assertEquals(before.covariance[0, 0], pop.populationPrior.covariance[0, 0])
    }

    @Test
    fun `refit pulls populationPrior toward observed per-instance means`() {
        // Feed three instances with consistent positive weights; population prior should
        // pick up the shared signal.
        val pop = HierarchicalBayesianRegression(featureSize = 2, initialPriorVariance = 1.0)
        val rng = Random(1)
        repeat(3) {
            val inst = pop.createInstance()
            repeat(200) {
                val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
                inst.update(x, y = 2.0 * x[0] - 1.0 * x[1], weight = 1.0)
            }
        }
        pop.refit()
        val mean = pop.populationPrior.mean
        // After refit, population mean should be near (2.0, -1.0)
        assertTrue(abs(mean[0] - 2.0) < 0.5, "population w[0] = ${mean[0]}, expected ~2.0")
        assertTrue(abs(mean[1] - (-1.0)) < 0.5, "population w[1] = ${mean[1]}, expected ~-1.0")
        assertEquals(3, pop.populationPrior.instanceCount)
    }

    @Test
    fun `new instance after refit inherits the refitted prior`() {
        val pop = HierarchicalBayesianRegression(featureSize = 2, initialPriorVariance = 1.0)
        val rng = Random(1)
        val first = pop.createInstance()
        repeat(300) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
            first.update(x, y = 3.0 * x[0] + 1.5 * x[1], weight = 1.0)
        }
        pop.refit()
        val second = pop.createInstance()
        val secondSnap = second.read()
        // Second instance starts with weights = population mean (refitted from first)
        assertTrue(abs(secondSnap.weights[0] - pop.populationPrior.mean[0]) < 1e-6)
        assertTrue(abs(secondSnap.weights[1] - pop.populationPrior.mean[1]) < 1e-6)
    }

    @Test
    fun `untrack stops contribution to refit`() {
        val pop = HierarchicalBayesianRegression(featureSize = 2, initialPriorVariance = 1.0)
        val rng = Random(1)
        // First instance learns w ~ (5, 5)
        val outlier = pop.createInstance()
        repeat(300) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
            outlier.update(x, y = 5.0 * x[0] + 5.0 * x[1], weight = 1.0)
        }
        // Two more instances learn w ~ (1, 1)
        repeat(2) {
            val inst = pop.createInstance()
            repeat(300) {
                val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
                inst.update(x, y = 1.0 * x[0] + 1.0 * x[1], weight = 1.0)
            }
        }
        pop.untrack(outlier)
        pop.refit()
        val mean = pop.populationPrior.mean
        // Without the outlier the prior is pulled toward (1, 1)
        assertTrue(abs(mean[0] - 1.0) < 0.5, "without outlier w[0] = ${mean[0]}, expected ~1.0")
        assertTrue(abs(mean[1] - 1.0) < 0.5, "without outlier w[1] = ${mean[1]}, expected ~1.0")
    }

    @Test
    fun `caller-supplied initial prior is honoured`() {
        val seedMean = DenseVector.of(doubleArrayOf(7.0, -3.0))
        val pop = HierarchicalBayesianRegression(
            featureSize = 2,
            initialPriorMean = seedMean,
        )
        val inst = pop.createInstance()
        val w = inst.read().weights
        assertEquals(7.0, w[0])
        assertEquals(-3.0, w[1])
    }

    @Test
    fun `Logit link propagates to instances`() {
        val pop = HierarchicalBayesianRegression(featureSize = 2, link = Link.Logit)
        val inst = pop.createInstance()
        assertEquals(Link.Logit, inst.link)
    }
}
