package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorLike
import com.eignex.koblas.forEachStored
import com.eignex.koblas.koblas

/** Writes `matrix * x` to [destination] without materialising a vector for sparse or generic [x]. */
internal fun DenseMatrix.multiplyInto(x: VectorLike, destination: DoubleArray) {
    require(x.size == cols) { "x size ${x.size} must match matrix columns $cols" }
    require(destination.size == rows) { "destination size ${destination.size} must match matrix rows $rows" }
    if (x is DenseVector) {
        koblas.gemv(1.0, this, x.data, 0.0, destination)
        return
    }
    destination.fill(0.0)
    x.forEachStored { column, value ->
        if (value != 0.0) {
            for (row in 0 until rows) destination[row] += this[row, column] * value
        }
    }
}
