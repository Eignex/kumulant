package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.kumulant.math.CholeskyFactor
import com.eignex.kumulant.math.factorCholesky
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Blackhole
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CholeskySolveBenchmark {
    @Param("32x1", "64x16", "128x1", "128x64", "256x16", "512x64", "1024x256")
    lateinit var shape: String

    private lateinit var factor: CholeskyFactor
    private lateinit var right: DenseMatrix

    @Setup
    fun setup() {
        val (size, columns) = shape.split('x').map(String::toInt)
        factor = CholeskyFixtures.matrix("rbf:$size").factorCholesky()
        right = CholeskyFixtures.right(size, columns)
    }

    @Benchmark
    fun solveMatrix(): DenseMatrix = factor.solve(right)

    @Benchmark
    fun solveLower(): DenseMatrix = factor.solveLower(right)

    @Benchmark
    fun solveLowerTranspose(): DenseMatrix = factor.solveLower(right, transpose = true)

    @Benchmark
    fun candidateBatches(blackhole: Blackhole) {
        repeat(16) { blackhole.consume(factor.solveLower(right)) }
    }
}
