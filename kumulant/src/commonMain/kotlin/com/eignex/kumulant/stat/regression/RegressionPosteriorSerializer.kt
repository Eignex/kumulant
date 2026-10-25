package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.schema.serialization.CanonicalRegressionPosteriorSerializer
import kotlinx.serialization.KSerializer

/** Serializes built-in [RegressionPosterior] implementations without a caller-supplied module. */
object RegressionPosteriorSerializer : KSerializer<RegressionPosterior<*>> by CanonicalRegressionPosteriorSerializer
