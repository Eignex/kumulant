package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.VectorExpr
import com.eignex.kumulant.schema.expr.Y
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// Names travel with the complete recipe; materialization delegates to the inner composition.
@Serializable
@SerialName("NamedSeries")
internal data class NamedSeries(val name: String, val inner: StatSpec) : SeriesStatSpec<Result> {
    init {
        require(name.isNotBlank()) { "A composition name must not be blank" }
    }
}

@Serializable
@SerialName("NamedPaired")
internal data class NamedPaired(val name: String, val inner: StatSpec) : PairedStatSpec<Result> {
    init {
        require(name.isNotBlank()) { "A composition name must not be blank" }
    }
}

@Serializable
@SerialName("NamedVector")
internal data class NamedVector(val name: String, val inner: StatSpec) : VectorStatSpec<Result> {
    init {
        require(name.isNotBlank()) { "A composition name must not be blank" }
    }
}

@Serializable
@SerialName("NamedDiscrete")
internal data class NamedDiscrete(val name: String, val inner: StatSpec) : DiscreteStatSpec<Result> {
    init {
        require(name.isNotBlank()) { "A composition name must not be blank" }
    }
}

@Serializable
@SerialName("NamedRegression")
internal data class NamedRegression(val name: String, val inner: StatSpec) : RegressionStatSpec<Result> {
    init {
        require(name.isNotBlank()) { "A composition name must not be blank" }
    }
}

@Serializable
internal enum class ResultProjection { Mad, Covariance, PlattCalibration, Accuracy }

@Serializable
@SerialName("ProjectSeries")
internal data class ProjectSeries(val inner: StatSpec, val projection: ResultProjection) : SeriesStatSpec<Result>

@Serializable
@SerialName("ProjectPaired")
internal data class ProjectPaired(val inner: StatSpec, val projection: ResultProjection) : PairedStatSpec<Result>

@Serializable
@SerialName("RegressionAsPaired")
internal data class RegressionAsPaired(val inner: StatSpec, val features: VectorExpr, val response: ScalarExpr = Y) :
    PairedStatSpec<Result>
