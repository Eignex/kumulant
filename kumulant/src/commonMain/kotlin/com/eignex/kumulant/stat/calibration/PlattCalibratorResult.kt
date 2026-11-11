package com.eignex.kumulant.stat.calibration

import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.stat.regression.glm.sigmoid
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Snapshot from [PlattCalibratorStat]: the learned sigmoid parameters and a
 * helper that maps a raw classifier score to a calibrated probability.
 *
 * The calibration map is `1 / (1 + exp(-(slope * x + intercept)))`.
 */
@Serializable
@SerialName("PlattCalibratorResult")
data class PlattCalibratorResult(
    /** Sigmoid slope; positive when higher raw scores correlate with positive labels. */
    val slope: Double,
    /** Sigmoid intercept. */
    val intercept: Double,
    override val totalWeights: Double,
    /** Optimizer steps taken, carried so a merged calibrator resumes its learning-rate schedule. */
    val step: Long = 0L,
) : HasObservationCount {

    /** Calibrated probability for raw score [x] via `sigmoid(slope * x + intercept)`. */
    fun calibrate(x: Double): Double = sigmoid(slope * x + intercept)
}
