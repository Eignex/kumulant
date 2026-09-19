package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.Vector
import com.eignex.koblas.forEachStored
import com.eignex.koblas.gemvInto

/** Writes `matrix * x` to [destination] without materialising a vector for sparse or generic [x]. */
internal fun DenseMatrix.multiplyInto(x: Vector, destination: DoubleArray) {
    require(x.size == cols) { "x size ${x.size} must match matrix columns $cols" }
    require(destination.size == rows) { "destination size ${destination.size} must match matrix rows $rows" }
    if (x is DenseVector) {
        // gemvInto rather than the raw-array gemv: a DenseVector may be strided, and only the typed entry
        // point carries its origin and step through to the library.
        gemvInto(1.0, x, 0.0, destination)
        return
    }
    destination.fill(0.0)
    x.forEachStored { column, value ->
        if (value != 0.0) {
            for (row in 0 until rows) destination[row] += this[row, column] * value
        }
    }
}
