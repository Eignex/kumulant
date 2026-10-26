package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace
import com.eignex.koblas.borrow
import com.eignex.koblas.koblas

// The Native comparison favors contiguous blocks over borrowed-region updates.
internal actual fun updateCholeskyTrailing(out: DenseMatrix, start: Int, width: Int, workspace: Workspace?) {
    val first = start + width
    val height = out.rows - first
    if (height == 0) return
    workspace.borrow(height * width) { panelValues ->
        val panel = DenseMatrix.wrap(height, width, panelValues)
        out.copyBlockInto(first, start, panel)
        workspace.borrow(height * height) { trailingValues ->
            val trailing = DenseMatrix.wrap(height, height, trailingValues)
            out.copyBlockInto(first, first, trailing)
            koblas.syrk(-1.0, panel, transpose = false, 1.0, trailing, lower = true, workspace = workspace)
            out.setBlock(first, first, trailing)
        }
    }
}

private fun DenseMatrix.copyBlockInto(r0: Int, c0: Int, out: DenseMatrix) {
    for (j in 0 until out.cols) {
        val from = (c0 + j) * rows + r0
        values.copyInto(out.values, j * out.rows, from, from + out.rows)
    }
}

private fun DenseMatrix.setBlock(r0: Int, c0: Int, src: DenseMatrix) {
    for (j in 0 until src.cols) {
        src.values.copyInto(values, (c0 + j) * rows + r0, j * src.rows, (j + 1) * src.rows)
    }
}
