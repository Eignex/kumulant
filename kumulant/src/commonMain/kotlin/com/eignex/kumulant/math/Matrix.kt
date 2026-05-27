package com.eignex.kumulant.math

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ArraySerializer
import kotlinx.serialization.builtins.DoubleArraySerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

/**
 * Read-only N-by-M matrix. Sealed alongside [VectorView] so snapshots
 * round-trip through `kotlinx.serialization` with their concrete storage
 * preserved. Public surface is read-only — shape, entry access, materialise
 * to `Array<DoubleArray>`. Mutation, factorisations, and arithmetic are
 * `internal` to kumulant.
 *
 * Surfaced primarily by the full-covariance regression results:
 * [com.eignex.kumulant.stat.regression.glm.CovarianceRegressionResult]
 * carries the posterior covariance and its Cholesky factor as `MatrixView`s
 * so a downstream consumer can sample from the joint posterior without
 * recomputing the decomposition.
 *
 * Only [DenseMatrix] today. A CSR/CSC sparse matrix can land here when a
 * consumer needs it; the only callers today (full-covariance regression
 * stats) have intrinsically dense state.
 */
@Serializable
sealed interface MatrixView {
    /** Number of rows. */
    val rows: Int

    /** Number of columns. */
    val cols: Int

    /** Read entry at row [i], column [j]. */
    operator fun get(i: Int, j: Int): Double

    /**
     * Materialise into a fresh row-major `Array<DoubleArray>`. Always
     * allocates; the result is independent of any internal storage.
     */
    fun toArray(): Array<DoubleArray>
}

/**
 * Dense row-major matrix backed by a single contiguous `DoubleArray` of
 * length `rows * cols`. Element `(i, j)` lives at `data[i * cols + j]`.
 *
 * Flat layout buys three properties: one heap allocation rather than
 * `rows` separate row arrays; cache-friendly sweeps across row boundaries;
 * the SIMD primitives in the internal `Primitives.kt` can stream long runs
 * without re-fetching row references on each iteration.
 *
 * The on-the-wire form is a 2D `Array<DoubleArray>` for readability when
 * inspecting JSON / CBOR payloads. The in-memory form is flat. The custom
 * [DenseMatrixSerializer] bridges the two — encoding writes a 2D array,
 * decoding reads one back and packs it into the flat backing.
 *
 * Mutation is `internal` — `DenseMatrix` is effectively immutable from
 * outside the kumulant module.
 */
@Serializable(with = DenseMatrixSerializer::class)
@SerialName("DenseMatrix")
class DenseMatrix internal constructor(
    override val rows: Int,
    override val cols: Int,
    internal val data: DoubleArray,
) : MatrixView {

    constructor(rows: Int, cols: Int = rows) : this(rows, cols, DoubleArray(rows * cols))

    init {
        require(rows >= 0 && cols >= 0) { "negative shape: ${rows}x$cols" }
        require(data.size == rows * cols) {
            "data length ${data.size} does not match shape ${rows}x$cols (= ${rows * cols})"
        }
    }

    override fun get(i: Int, j: Int): Double = data[i * cols + j]
    override fun toArray(): Array<DoubleArray> = Array(rows) { i ->
        DoubleArray(cols) { j -> data[i * cols + j] }
    }

    internal operator fun set(i: Int, j: Int, v: Double) {
        data[i * cols + j] = v
    }

    /** Offset into [data] where row [i] starts. */
    internal fun rowOffset(i: Int): Int = i * cols

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DenseMatrix) return false
        return rows == other.rows && cols == other.cols && data.contentEquals(other.data)
    }
    override fun hashCode(): Int {
        var h = rows * 31 + cols
        h = 31 * h + data.contentHashCode()
        return h
    }
    override fun toString(): String = "DenseMatrix(${rows}x$cols)"

    /** Factory entrypoints for [DenseMatrix]. */
    companion object {
        /** Copy a row-major `Array<DoubleArray>` into a fresh dense matrix. */
        fun of(rows: Array<DoubleArray>): DenseMatrix {
            val r = rows.size
            val c = if (r == 0) 0 else rows[0].size
            require(rows.all { it.size == c }) { "all rows must have the same length" }
            val flat = DoubleArray(r * c)
            for (i in 0 until r) rows[i].copyInto(flat, destinationOffset = i * c)
            return DenseMatrix(r, c, flat)
        }

        /** Create an NxN identity matrix scaled by [diagonal]. */
        fun diagonal(size: Int, diagonal: Double = 1.0): DenseMatrix {
            val m = DenseMatrix(size, size)
            for (i in 0 until size) m[i, i] = diagonal
            return m
        }

        /** Wrap an existing flat `DoubleArray` of length `rows * cols` without copying. */
        internal fun wrap(rows: Int, cols: Int, data: DoubleArray): DenseMatrix = DenseMatrix(rows, cols, data)
    }
}

/** Serialises [DenseMatrix] as a 2D `Array<DoubleArray>`. The flat in-memory backing
 *  is an implementation detail; the wire shape stays stable across layout changes. */
@OptIn(ExperimentalSerializationApi::class)
internal object DenseMatrixSerializer : KSerializer<DenseMatrix> {
    private val inner = ArraySerializer(DoubleArraySerializer())
    override val descriptor: SerialDescriptor get() = inner.descriptor
    override fun serialize(encoder: Encoder, value: DenseMatrix) =
        encoder.encodeSerializableValue(inner, value.toArray())
    override fun deserialize(decoder: Decoder): DenseMatrix = DenseMatrix.of(decoder.decodeSerializableValue(inner))
}
