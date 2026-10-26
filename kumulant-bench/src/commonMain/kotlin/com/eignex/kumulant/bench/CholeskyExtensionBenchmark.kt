package com.eignex.kumulant.bench

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.kumulant.math.CholeskyFactor
import com.eignex.kumulant.math.factorCholesky
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class CholeskyExtensionBenchmark {
    @Param("16x1", "64x4", "128x16", "512x1", "512x4", "512x16", "1024x16")
    lateinit var extensionShape: String

    private lateinit var factor: CholeskyFactor
    private lateinit var cross: DenseMatrix
    private lateinit var diagonal: DenseMatrix
    private lateinit var full: DenseMatrix
    private val workspace = Workspace()

    @Setup
    fun setup() {
        reportCholeskyBackend()
        val (size, added) = extensionShape.split('x').map(String::toInt)
        val fixture = CholeskyFixtures.extension(size, added)
        full = fixture.full
        cross = fixture.cross
        diagonal = fixture.diagonal
        factor = fixture.base.factorCholesky()
    }

    @Benchmark
    fun extend(): CholeskyFactor = factor.extended(cross, diagonal)

    @Benchmark
    fun extendWithWorkspace(): CholeskyFactor = factor.extended(cross, diagonal, workspace)

    @Benchmark
    fun refactor(): CholeskyFactor = full.factorCholesky(workspace)

    @Benchmark
    fun factorCopy(): DenseMatrix = factor.lowerFactor()
}
