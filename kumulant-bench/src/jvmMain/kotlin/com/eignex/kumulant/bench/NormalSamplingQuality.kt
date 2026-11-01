package com.eignex.kumulant.bench

import com.eignex.kumulant.math.SobolSequence
import com.eignex.kumulant.math.nextNormal
import com.eignex.kumulant.schema.ops.mseLoss
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.stat.summary.MeanStat
import kotlin.math.cos
import kotlin.math.exp
import kotlin.math.sqrt
import kotlin.random.Random

internal fun analyzeNormalSamplingQuality() {
    println("normal_sampling,samples,dimensions,rmse; integral=cos(0.2*sum(normal)); seeds=0..7")
    for (dimensions in listOf(4, 32, 128)) {
        val expected = exp(-0.02 * dimensions)
        for (samples in listOf(256, 1024, 4096)) {
            for (sobol in listOf(false, true)) {
                val error = mseLoss().materialize()
                repeat(8) { seed ->
                    val random = Random(seed)
                    val sequence = SobolSequence(dimensions, seed.toLong())
                    val point = DoubleArray(dimensions)
                    val mean = MeanStat()
                    repeat(samples) {
                        if (sobol) sequence.nextNormal(point) else {
                            for (coordinate in point.indices) point[coordinate] = random.nextNormal()
                        }
                        mean.update(cos(0.2 * point.sum()), 0L)
                    }
                    error.update(mean.read(0L).mean, expected, 0L)
                }
                println("${if (sobol) "sobol" else "pseudo"},$samples,$dimensions,${sqrt(error.read(0L).mean)}")
            }
        }
    }
}
