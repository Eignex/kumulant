@file:OptIn(com.eignex.koblas.KoblasEngineApi::class)

package com.eignex.kumulant.bench

import com.eignex.koblas.BuiltinEngines
import com.eignex.koblas.koblas
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.factorCholesky
import java.nio.ByteBuffer
import java.nio.file.Path
import java.security.MessageDigest
import kotlinx.serialization.encodeToString
import kotlin.math.abs

fun main(args: Array<String>) {
    require(args.size == 4) { "Expected preset, case ID, target, and output file" }
    val case = captureCases(args[0]).single { it.id == args[1] }
    val expected = when (args[2]) {
        "jvm-scalar" -> BuiltinEngines.scalar
        "jvm-simd" -> requireNotNull(BuiltinEngines.simd) { "SIMD backend is unavailable" }
        else -> error("Unknown target: ${args[2]}")
    }
    check(koblas === expected) { "Requested ${args[2]}, resolved ${koblas.name}" }
    val validation = captureQuality(case)
    writeCaptureJson(Path.of(args[3]), captureJson.encodeToString(validation))
    check(validation.passed) { "Quality validation failed for ${case.id}" }
}

internal fun captureQuality(case: CaptureCase): CaptureValidation = when (case.suite) {
    "stats" -> statQuality(allSpecs.single { it.name == case.parameters.getValue("name") }, case)
    "cholesky" -> choleskyQuality(case)
    else -> error("No quality driver for ${case.suite}")
}

private fun <S, R : Result> statQuality(spec: StatSpec<S, R>, case: CaptureCase): CaptureValidation {
    val updates = spec.updates(case.seed, case.batchSize).toList()
    val fingerprint = CaptureFingerprint()
    for (update in updates) {
        fingerprint.add(update.value.toRawBits())
        fingerprint.add(update.weight.toRawBits())
        fingerprint.add(update.timestampNanos)
    }
    val stat = spec.factory(Concurrency.None)
    for (update in updates) spec.applyUpdate(stat, update)
    val actual = spec.scalar(spec.readSnapshot(stat, spec.readAt(updates.size)))
    val expected = spec.reference(updates.asSequence())
    return CaptureValidation(
        backend = koblas.name,
        inputSha256 = fingerprint.finish(),
        metrics = listOf(
            CaptureMetric("actual", actual, "value"),
            CaptureMetric("reference", expected, "value"),
            CaptureMetric("scaledAbsoluteError", abs(actual - expected) / maxOf(1.0, abs(expected)), "ratio", 1e-10),
        ),
    )
}

private fun choleskyQuality(case: CaptureCase): CaptureValidation {
    val shape = case.parameters["shape"]?.split('x')?.map(String::toInt)
    val matrix = CholeskyFixtures.matrix(case.parameters["matrixCase"] ?: "rbf:${shape!![0]}")
    val right = CholeskyFixtures.right(matrix.rows, shape?.get(1) ?: 1)
    val fingerprint = CaptureFingerprint()
    fingerprint.add(matrix.rows.toLong())
    fingerprint.add(right.cols.toLong())
    matrix.values.forEach { fingerprint.add(it.toRawBits()) }
    right.values.forEach { fingerprint.add(it.toRawBits()) }
    val factor = matrix.factorCholesky()
    val solution = factor.solve(right)
    return CaptureValidation(
        backend = koblas.name,
        inputSha256 = fingerprint.finish(),
        metrics = listOf(
            CaptureMetric("relativeReconstructionError", relativeReconstructionError(matrix, factor.lowerFactor()), "ratio", 1e-13),
            CaptureMetric("solveBackwardError", solveBackwardError(matrix, right, solution), "ratio", 1e-13),
            CaptureMetric("logDeterminant", factor.logDeterminant(), "value"),
        ),
    )
}

internal class CaptureFingerprint {
    private val digest = MessageDigest.getInstance("SHA-256")
    private val buffer = ByteBuffer.allocate(Long.SIZE_BYTES)

    fun add(value: Long) {
        buffer.clear()
        buffer.putLong(value)
        digest.update(buffer.array())
    }

    fun finish(): String = digest.digest().joinToString("") { "%02x".format(it) }
}
