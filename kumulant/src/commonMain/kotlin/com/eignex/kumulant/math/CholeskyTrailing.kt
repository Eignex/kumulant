package com.eignex.kumulant.math

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.Workspace

internal expect fun updateCholeskyTrailing(out: DenseMatrix, start: Int, width: Int, workspace: Workspace?)
