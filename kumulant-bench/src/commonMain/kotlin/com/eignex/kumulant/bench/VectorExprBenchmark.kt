package com.eignex.kumulant.bench

import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64StridedVectorView
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.schema.expr.V
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.VFoldOp
import com.eignex.kumulant.schema.expr.eval
import com.eignex.kumulant.schema.expr.gt
import com.eignex.kumulant.schema.expr.plus
import com.eignex.kumulant.schema.expr.vDot
import com.eignex.kumulant.schema.expr.vFold
import com.eignex.kumulant.schema.expr.vectorOf
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State

@State(Scope.Benchmark)
open class VectorExprBenchmark {
    @Param("8", "32", "128", "512")
    var featureSize: Int = 8

    @Param("1", "10", "100")
    var densityPercent: Int = 1

    @Param("dense", "sparse", "strided", "custom")
    lateinit var representation: String

    private lateinit var input: F64VectorLike
    private val scalar = V(0) + V(1)
    private val predicate = V(0) gt 0.0
    private val vector = vectorOf(V(0), V(1))
    private val sum = vFold(VFoldOp.Sum)
    private val mean = vFold(VFoldOp.Mean)
    private val minimum = vFold(VFoldOp.Min)
    private val maximum = vFold(VFoldOp.Max)
    private val norm2 = vFold(VFoldOp.Norm2)
    private lateinit var dot: ScalarExpr

    @Setup
    fun setup() {
        val values = DoubleArray(featureSize) { if (it % 3 == 0) 1.0 else 0.0 }
        input = when (representation) {
            "dense" -> F64DenseVector.of(values)
            "sparse" -> {
                val nnz = (featureSize * densityPercent / 100).coerceAtLeast(1)
                F64SparseVector.of(featureSize, IntArray(nnz) { it * featureSize / nnz }, DoubleArray(nnz) { 1.0 })
            }
            "strided" -> F64StridedVectorView(DoubleArray(featureSize * 2) { values[it / 2] }, 0, featureSize, 2)
            else -> Vector(values)
        }
        dot = vDot(List(featureSize) { if (it % 2 == 0) 1.0 else -1.0 })
    }

    @Benchmark fun scalar(): Double = scalar.eval(v = input)
    @Benchmark fun predicate(): Boolean = predicate.eval(v = input)
    @Benchmark fun vector(): DoubleArray = vector.eval(v = input)
    @Benchmark fun sum(): Double = sum.eval(v = input)
    @Benchmark fun mean(): Double = mean.eval(v = input)
    @Benchmark fun minimum(): Double = minimum.eval(v = input)
    @Benchmark fun maximum(): Double = maximum.eval(v = input)
    @Benchmark fun norm2(): Double = norm2.eval(v = input)
    @Benchmark fun dot(): Double = dot.eval(v = input)
    @Benchmark fun materializedScalar(): Double = scalar.eval(0.0, 0.0, input.toDoubleArray())
    @Benchmark fun materializedPredicate(): Boolean = predicate.eval(0.0, 0.0, input.toDoubleArray())
    @Benchmark fun materializedVector(): DoubleArray = vector.eval(0.0, 0.0, input.toDoubleArray())

    private class Vector(private val values: DoubleArray) : F64VectorLike {
        override val size: Int get() = values.size
        override fun get(i: Int): Double = values[i]
        override fun toDoubleArray(): DoubleArray = values.copyOf()
    }
}
