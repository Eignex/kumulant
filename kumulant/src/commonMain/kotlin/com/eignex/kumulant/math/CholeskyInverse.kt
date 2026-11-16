package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Scratch
import com.eignex.koblas.koblas

private const val INVERSE_PANEL_WIDTH = 64

internal fun DenseMatrix.invertCholeskyPanels(out: DenseMatrix, workspace: Scratch): DenseMatrix {
    val n = rows
    out.values.fill(0.0)
    var start = 0
    while (start < n) {
        val height = n - start
        val width = minOf(INVERSE_PANEL_WIDTH, height)
        val factor = view(start, start, height, height)
        val right = out.view(start, start, height, width)
        for (j in 0 until width) right[j, j] = 1.0
        // Column j has no forward-solve entries above j. The trailing factor therefore suffices
        // for its lower inverse entries; batching keeps those solves in matrix kernels.
        koblas.trsm(factor, right, lower = true, scratch = workspace)
        koblas.trsm(factor, right, lower = true, transpose = true, scratch = workspace)
        start += width
    }
    for (j in 0 until n) {
        for (i in j + 1 until n) out[j, i] = out[i, j]
    }
    return out
}
