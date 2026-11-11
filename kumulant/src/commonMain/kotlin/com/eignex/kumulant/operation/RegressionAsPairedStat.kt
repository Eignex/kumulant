package com.eignex.kumulant.operation

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.VectorExpr

internal class RegressionAsPairedStat<R : Result>(
    private val delegate: RegressionStat<R>,
    private val features: VectorExpr,
    private val response: ScalarExpr,
) : PairedStat<R>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(DenseVector.of(features.eval(x, y)), response.eval(x, y), timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        RegressionAsPairedStat(delegate.create(concurrency), features, response)
}
