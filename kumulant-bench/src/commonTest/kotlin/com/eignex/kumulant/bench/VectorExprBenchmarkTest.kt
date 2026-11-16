package com.eignex.kumulant.bench

import kotlin.test.Test
import kotlin.test.assertEquals

class VectorExprBenchmarkTest {
    @Test
    fun `scalar dot matches expression dot across input representations`() {
        for (size in listOf(8, 512)) {
            for (density in listOf(1, 100)) {
                for (kind in listOf("dense", "sparse", "strided", "custom")) {
                    val benchmark = VectorExprBenchmark().apply {
                        featureSize = size
                        densityPercent = density
                        representation = kind
                        setup()
                    }

                    assertEquals(benchmark.dot(), benchmark.scalarDot(), "size=$size density=$density kind=$kind")
                }
            }
        }
    }
}
