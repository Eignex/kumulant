package com.eignex.kumulant.bench

import com.eignex.koblas.dense.DenseCall
import com.eignex.koblas.dense.DenseMatrixOperation
import com.eignex.koblas.koblas

fun main(args: Array<String>) {
    reportCholeskyBackend()
    if (args.isNotEmpty()) {
        analyzeCholeskyStress(args.singleOrNull()?.toIntOrNull() ?: error("Usage: choleskyQuality [STRESS_ORDER]"))
        return
    }
    for (size in listOf(128, 512, 1024)) {
        val width = minOf(64, size / 4)
        for (borrowed in listOf(false, true)) {
            val call = DenseCall(
                rows = size - width, columns = size - width, depth = width,
                alpha = -1.0, borrowed = borrowed,
            )
            println("Native first trailing update: order=$size borrowed=$borrowed route=${koblas.routeOf(DenseMatrixOperation.Syrk, call)}")
        }
    }
    analyzeCholeskyAccuracy()
}
