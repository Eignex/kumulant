@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema.ops

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.VectorExpr
import com.eignex.kumulant.schema.expr.Y
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.calibration.PlattCalibratorResult
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.score.AccuracyResult
import com.eignex.kumulant.stat.summary.MadResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/** Give a composition a wire-portable name while retaining its full recipe and result type. */
fun <R : Result> SeriesStatSpec<R>.named(name: String): SeriesStatSpec<R> = NamedSeries(name, this) as SeriesStatSpec<R>

/** Give a paired composition a wire-portable name while retaining its full recipe and result type. */
fun <R : Result> PairedStatSpec<R>.named(name: String): PairedStatSpec<R> = NamedPaired(name, this) as PairedStatSpec<R>

/** Give a vector composition a wire-portable name while retaining its full recipe and result type. */
fun <R : Result> VectorStatSpec<R>.named(name: String): VectorStatSpec<R> = NamedVector(name, this) as VectorStatSpec<R>

/** Give a discrete composition a wire-portable name while retaining its full recipe and result type. */
fun <R : Result> DiscreteStatSpec<R>.named(name: String): DiscreteStatSpec<R> = NamedDiscrete(
    name,
    this,
) as DiscreteStatSpec<R>

/** Give a regression composition a wire-portable name while retaining its full recipe and result type. */
fun <R : Result> RegressionStatSpec<R>.named(name: String): RegressionStatSpec<R> =
    NamedRegression(name, this) as RegressionStatSpec<R>

/** Report median and unscaled MAD, retaining the raw digest for subsequent merges. */
fun SeriesStatSpec<TDigestResult>.medianAbsoluteDeviation(): SeriesStatSpec<MadResult> =
    ProjectSeries(this, ResultProjection.Mad) as SeriesStatSpec<MadResult>

/** Report covariance and correlation, retaining the marginal moments for subsequent merges. */
fun PairedStatSpec<UnivariateRegressionResult>.covariance(): PairedStatSpec<CovarianceResult> =
    ProjectPaired(this, ResultProjection.Covariance) as PairedStatSpec<CovarianceResult>

/** Report a one-feature logit model as a sigmoid calibrator, retaining its learning-rate step. */
fun PairedStatSpec<StochasticRegressionResult>.plattCalibration(): PairedStatSpec<PlattCalibratorResult> =
    ProjectPaired(this, ResultProjection.PlattCalibration) as PairedStatSpec<PlattCalibratorResult>

/** Report the mean of binary correctness observations as a named classification-accuracy result. */
fun PairedStatSpec<WeightedMeanResult>.classificationAccuracy(): PairedStatSpec<AccuracyResult> =
    ProjectPaired(this, ResultProjection.Accuracy) as PairedStatSpec<AccuracyResult>

/** Feed scalar pairs to a regression recipe through portable feature and response expressions. */
fun <R : Result> RegressionStatSpec<R>.asPaired(features: VectorExpr, response: ScalarExpr = Y): PairedStatSpec<R> =
    RegressionAsPaired(this, features, response) as PairedStatSpec<R>
