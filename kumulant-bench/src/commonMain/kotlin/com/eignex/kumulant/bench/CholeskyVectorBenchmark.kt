package com.eignex.kumulant.bench

import com.eignex.koblas.Workspace
import com.eignex.kumulant.math.CholeskyFactor
import com.eignex.kumulant.math.factorCholesky
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CholeskyVectorBenchmark {
    @Param("32", "128", "512", "1024")
    var historySize: Int = 32

    private lateinit var factor: CholeskyFactor
    private lateinit var right: DoubleArray
    private lateinit var out: DoubleArray
    private val workspace = Workspace()

    @Setup
    fun setup() {
        factor = CholeskyFixtures.matrix("rbf:$historySize").factorCholesky()
        right = CholeskyFixtures.right(historySize).values
        out = DoubleArray(historySize)
    }

    @Benchmark
    fun solveVector(): DoubleArray = factor.solve(right)

    @Benchmark
    fun solveVectorInto(): DoubleArray = factor.solveInto(right, out, workspace)

    @Benchmark
    fun logDeterminant(): Double = factor.logDeterminant()
}
