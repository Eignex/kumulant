package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.koblas.koblas

internal actual fun updateCholeskyTrailing(out: DenseMatrix, start: Int, width: Int, workspace: Workspace?) {
    val first = start + width
    val height = out.rows - first
    if (height == 0) return
    val panel = out.view(first, start, height, width)
    val trailing = out.view(first, first, height, height)
    koblas.syrk(-1.0, panel, transpose = false, 1.0, trailing, lower = true, workspace = workspace)
}
