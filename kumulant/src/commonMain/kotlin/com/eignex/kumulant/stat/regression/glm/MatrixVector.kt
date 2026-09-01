package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.forEachStored
import com.eignex.koblas.koblas

/** Writes `matrix * x` to [destination] without materialising a vector for sparse or generic [x]. */
internal fun F64DenseMatrix.multiplyInto(x: F64VectorLike, destination: DoubleArray) {
    require(x.size == cols) { "x size ${x.size} must match matrix columns $cols" }
    require(destination.size == rows) { "destination size ${destination.size} must match matrix rows $rows" }
    if (x is F64DenseVector) {
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
