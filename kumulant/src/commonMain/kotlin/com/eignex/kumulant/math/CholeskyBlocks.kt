/*
 * Copyright 2009, 2010 The University of Texas at Austin.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 * copyright notice, this list of conditions and the following
 * disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials
 * provided with the distribution.
 *
 * THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT
 * AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,
 * INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF
 * MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE
 * DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT
 * AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE
 * GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF
 * LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT
 * OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and
 * documentation are those of the authors and should not be
 * interpreted as representing official policies, either expressed
 * or implied, of The University of Texas at Austin.
 */

@file:OptIn(com.eignex.koblas.ExperimentalKoblasApi::class)

package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.StridedVectorView
import com.eignex.koblas.Workspace
import com.eignex.koblas.borrow
import com.eignex.koblas.dense.Kernels
import com.eignex.koblas.dense.PackedPanels
import com.eignex.koblas.koblas
import com.eignex.koblas.view
import kotlin.math.min
import kotlin.math.sqrt

private const val CHOLESKY_LEAF = 16

// Adapted from OpenBLAS potrf_L_single.c and potf2_L.c
// at 632ef874379c16ca8646d9fa0f6c60449e47d960. Packing and compute leaves belong to koblas.
internal fun Kernels.supportsCholeskyPanels(): Boolean =
    gemmTileRows == PackedPanels.tileRows && gemmTileCols == PackedPanels.tileColumns

// Staging preserves the input block for full-column regularization if a trial pivot needs repair.
internal fun tryCholeskyBlock(
    kernels: Kernels,
    matrix: DenseMatrix,
    start: Int,
    width: Int,
    minimumPivot: Double,
    workspace: Workspace?,
): Boolean = workspace.borrow(width * width) { data ->
    val n = matrix.rows
    for (j in 0 until width) {
        val source = start + (start + j) * n
        matrix.data.copyInto(data, j * width, source, source + width)
    }
    val diagonal = DenseMatrix.wrap(width, width, data)
    if (!tryCholeskyDiagonal(kernels, diagonal, minimumPivot, workspace)) return@borrow false
    val end = start + width
    val height = n - end
    workspace.borrow(PackedPanels.leftSize(height, width)) { panel ->
        if (height > 0) {
            PackedPanels.packLeft(matrix, panel, height, width, sourceRow = end, sourceColumn = start)
            solveCholeskyPanel(kernels, diagonal, panel, height, workspace)
            if (!panel.all { it.isFinite() }) return@borrow false
        }
        for (j in 0 until width) data.copyInto(matrix.data, start + (start + j) * n, j * width, (j + 1) * width)
        if (height > 0) {
            PackedPanels.writeLeft(panel, matrix, height, width, destinationRow = end, destinationColumn = start)
            subtractCholeskyPanel(kernels, matrix, end, width, panel, workspace)
        }
        true
    }
}

private fun tryCholeskyDiagonal(
    kernels: Kernels,
    matrix: DenseMatrix,
    minimumPivot: Double,
    workspace: Workspace?,
): Boolean {
    val n = matrix.rows
    if (n <= CHOLESKY_LEAF) return tryCholeskyLeaf(kernels, matrix, minimumPivot, workspace)
    val block = n / 4
    var start = 0
    while (start < n) {
        val width = min(block, n - start)
        if (!tryCholeskyBlock(kernels, matrix, start, width, minimumPivot, workspace)) return false
        start += width
    }
    return true
}

private fun solveCholeskyPanel(
    kernels: Kernels,
    diagonal: DenseMatrix,
    panel: DoubleArray,
    height: Int,
    workspace: Workspace?,
) {
    val width = diagonal.rows
    val rows = kernels.gemmTileRows
    val columns = kernels.gemmTileCols
    workspace.borrow(PackedPanels.rightSize(width, width)) { triangle ->
        PackedPanels.packTriangularRight(diagonal, triangle, width, width, lower = true, transpose = true)
        var column = 0
        while (column < width) {
            val order = min(columns, width - column)
            val trianglePanel = column * width
            var row = 0
            while (row < height) {
                val validRows = min(rows, height - row)
                val rowPanel = row * width
                kernels.gemmTrsmTile(
                    column, validRows, order, panel, rowPanel, triangle, trianglePanel,
                    triangle, trianglePanel + column * columns, false, false, panel, rowPanel + column * rows,
                )
                row += rows
            }
            column += columns
        }
    }
    PackedPanels.clearLeftPadding(panel, height, width)
}

internal fun updateCholeskyTrailing(
    kernels: Kernels,
    matrix: DenseMatrix,
    start: Int,
    width: Int,
    workspace: Workspace?,
) {
    val end = start + width
    val height = matrix.rows - end
    if (height == 0) return
    workspace.borrow(PackedPanels.leftSize(height, width)) { panel ->
        PackedPanels.packLeft(matrix, panel, height, width, sourceRow = end, sourceColumn = start)
        subtractCholeskyPanel(kernels, matrix, end, width, panel, workspace)
    }
}

// The solved left panel remains packed after TRSM. Only its transposed right format needs packing for SYRK.
private fun subtractCholeskyPanel(
    kernels: Kernels,
    matrix: DenseMatrix,
    start: Int,
    width: Int,
    left: DoubleArray,
    workspace: Workspace?,
) {
    val height = matrix.rows - start
    val rows = kernels.gemmTileRows
    val columns = kernels.gemmTileCols
    kernels.scale(left, 0, -1.0, left.size)
    workspace.borrow(PackedPanels.rightSize(width, height)) { right ->
        PackedPanels.packRight(
            matrix,
            right,
            width,
            height,
            sourceRow = start - width,
            sourceColumn = start,
            transpose = true,
        )
        workspace.borrow(rows * columns) { edge ->
            var column = 0
            while (column < height) {
                var row = column / rows * rows
                while (row < height) {
                    val destination = start + row + (start + column) * matrix.rows
                    if (row >= column + columns - 1 && row + rows <= height && column + columns <= height) {
                        kernels.gemmTile(
                            width,
                            left,
                            row * width,
                            right,
                            column * width,
                            matrix.data,
                            destination,
                            matrix.rows,
                        )
                    } else {
                        // A diagonal or edge tile must preserve entries outside the stored lower triangle.
                        edge.fill(0.0)
                        kernels.gemmTile(width, left, row * width, right, column * width, edge, 0, rows)
                        for (j in 0 until min(columns, height - column)) {
                            for (i in 0 until min(rows, height - row)) {
                                if (row + i >= column + j) {
                                    matrix.data[destination + i + j * matrix.rows] += edge[i + j * rows]
                                }
                            }
                        }
                    }
                    row += rows
                }
                column += columns
            }
        }
    }
}

private fun tryCholeskyLeaf(
    kernels: Kernels,
    matrix: DenseMatrix,
    minimumPivot: Double,
    workspace: Workspace?,
): Boolean {
    val n = matrix.rows
    val data = matrix.data
    return workspace.borrow(n) { row ->
        for (j in 0 until n) {
            val base = j + j * n
            for (p in 0 until j) row[p] = data[j + p * n]
            val pivot = data[base] - kernels.dot(row, 0, row, 0, j)
            if (!pivot.isFinite() || pivot <= 0.0 || pivot < minimumPivot) return@borrow false
            val diagonal = sqrt(pivot)
            data[base] = diagonal
            val length = n - j - 1
            if (length > 0) {
                if (j > 0) {
                    koblas.blas.gemv(
                        -1.0,
                        matrix.view(j + 1, length, 0, j),
                        StridedVectorView(row, 0, j),
                        1.0,
                        StridedVectorView(data, base + 1, length),
                    )
                }
                kernels.scale(data, base + 1, 1.0 / diagonal, length)
                for (i in base + 1 until base + 1 + length) if (!data[i].isFinite()) return@borrow false
            }
        }
        true
    }
}
