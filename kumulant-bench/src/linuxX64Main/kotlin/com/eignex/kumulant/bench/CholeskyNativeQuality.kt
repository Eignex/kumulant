package com.eignex.kumulant.bench

fun main(args: Array<String>) {
    reportCholeskyBackend()
    if (args.isNotEmpty()) {
        analyzeCholeskyStress(args.singleOrNull()?.toIntOrNull() ?: error("Usage: choleskyQuality [STRESS_ORDER]"))
        return
    }
    analyzeCholeskyAccuracy()
}
