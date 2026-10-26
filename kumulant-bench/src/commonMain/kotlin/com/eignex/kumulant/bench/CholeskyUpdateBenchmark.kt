package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.kumulant.math.CholeskyFactor
import com.eignex.kumulant.math.factorCholesky
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Blackhole
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CholeskyUpdateBenchmark {
    @Param("8", "32", "128", "512")
    var featureSize: Int = 8

    private lateinit var matrix: DenseMatrix
    private lateinit var factor: CholeskyFactor
    private lateinit var updates: Array<DoubleArray>
    private lateinit var right: DoubleArray
    private lateinit var out: DoubleArray
    private val workspace = Workspace()

    @Setup
    fun setup() {
        reportCholeskyBackend()
        matrix = CholeskyFixtures.precision(featureSize)
        factor = matrix.factorCholesky()
        val observations = CholeskyFixtures.right(featureSize, 8)
        updates = Array(8) { observations.values.copyOfRange(it * featureSize, (it + 1) * featureSize) }
        right = CholeskyFixtures.right(featureSize).values
        out = DoubleArray(featureSize)
    }

    @Benchmark
    fun rankUpdate(): CholeskyFactor = factor.rankUpdated(updates[0])

    @Benchmark
    fun rankUpdateWithWorkspace(): CholeskyFactor = factor.rankUpdated(updates[0], workspace)

    @Benchmark
    fun refactorUpdate(): CholeskyFactor {
        val updated = DenseMatrix.wrap(featureSize, featureSize, matrix.values.copyOf())
        CholeskyFixtures.addOuterProduct(updated, updates[0])
        return updated.factorCholesky(workspace)
    }

    @Benchmark
    fun factorCopy(): DenseMatrix = factor.lowerFactor()

    @Benchmark
    fun posteriorUpdateSolve(blackhole: Blackhole) {
        var current = factor
        val information = right.copyOf()
        for ((index, update) in updates.withIndex()) {
            current = current.rankUpdated(update, workspace)
            for (i in information.indices) information[i] += update[i] * (index + 1) * 0.125
            blackhole.consume(current.solveInto(information, out, workspace))
        }
    }

    @Benchmark
    fun posteriorRefactorSolve(blackhole: Blackhole) {
        val updated = DenseMatrix.wrap(featureSize, featureSize, matrix.values.copyOf())
        val information = right.copyOf()
        for ((index, update) in updates.withIndex()) {
            CholeskyFixtures.addOuterProduct(updated, update)
            for (i in information.indices) information[i] += update[i] * (index + 1) * 0.125
            blackhole.consume(updated.factorCholesky(workspace).solveInto(information, out, workspace))
        }
    }

}
