package com.eignex.kumulant.bench

import com.eignex.kumulant.math.SobolSequence
import com.eignex.kumulant.math.nextNormal
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.Param
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State
import kotlin.random.Random

@State(Scope.Benchmark)
open class NormalSamplingBenchmark {
    @Param("256", "1024", "4096")
    var samples: Int = 256

    @Param("4", "32", "128")
    var dimensions: Int = 4

    @Param("pseudo", "sobol")
    lateinit var sampling: String

    private lateinit var sequence: SobolSequence
    private lateinit var random: Random
    private lateinit var point: DoubleArray
    private lateinit var output: DoubleArray

    @Setup
    fun setup() {
        sequence = SobolSequence(dimensions, 771L)
        random = Random(771)
        point = DoubleArray(dimensions)
        output = DoubleArray(samples * dimensions)
    }

    @Benchmark
    fun draw(): DoubleArray = draw(sequence, random)

    @Benchmark
    fun bindAndDraw(): DoubleArray = draw(
        if (sampling == "sobol") SobolSequence(dimensions, 771L) else sequence,
        if (sampling == "pseudo") Random(771) else random,
    )

    private fun draw(sequence: SobolSequence, random: Random): DoubleArray {
        repeat(samples) { sample ->
            if (sampling == "sobol") sequence.nextNormal(point) else {
                for (coordinate in point.indices) point[coordinate] = random.nextNormal()
            }
            point.copyInto(output, sample * dimensions)
        }
        return output
    }
}
