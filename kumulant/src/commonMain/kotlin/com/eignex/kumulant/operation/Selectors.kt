package com.eignex.kumulant.operation

import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat

internal fun <R : Result> SeriesStat<R>.atX(): PairedStat<R> = FoldPairedStat(this) { x, _ -> x }

internal fun <R : Result> SeriesStat<R>.atY(): PairedStat<R> = FoldPairedStat(this) { _, y -> y }

internal fun <R : Result> SeriesStat<R>.atIndex(index: Int): VectorStat<R> =
    FoldVectorStat.vector(this) { vector -> vector[index] }

internal fun <R : Result> PairedStat<R>.atIndices(indexX: Int, indexY: Int): VectorStat<R> =
    FoldVectorPairedStat.vector(this, { vector -> vector[indexX] }, { vector -> vector[indexY] })
