package com.eignex.kumulant.math

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Read-only N-vector. Backing storage may be dense or sparse — callers see the same
 * surface either way. The public contract is intentionally minimal: query the size,
 * read an entry by index, materialise to a `DoubleArray`. Everything else (dot
 * products, axpy, factorisations, mutation) is `internal` to kumulant; these views
 * are observation snapshots, not a public linear-algebra toolkit.
 *
 * Subtypes are sealed and serialisable so snapshots round-trip through
 * `kotlinx.serialization` with their concrete storage preserved (dense ↔ dense,
 * sparse ↔ sparse).
 */
@Serializable
sealed interface VectorView {
    val size: Int

    /** Read entry at [i]. O(1) for dense, O(nnz) linear scan for sparse. */
    operator fun get(i: Int): Double

    /** Materialise into a fresh dense `DoubleArray`. */
    fun toDoubleArray(): DoubleArray
}

/**
 * Dense double-precision vector. Mutation is `internal`; external callers only see
 * the read-only [VectorView] surface.
 */
@Serializable
@SerialName("DenseVector")
class DenseVector internal constructor(internal val data: DoubleArray) : VectorView {

    constructor(size: Int) : this(DoubleArray(size))

    override val size: Int get() = data.size
    override fun get(i: Int): Double = data[i]
    override fun toDoubleArray(): DoubleArray = data.copyOf()

    internal operator fun set(i: Int, v: Double) { data[i] = v }

    override fun equals(other: Any?): Boolean =
        this === other || (other is DenseVector && data.contentEquals(other.data))
    override fun hashCode(): Int = data.contentHashCode()
    override fun toString(): String = "DenseVector(size=$size)"

    companion object {
        /** Copy a `DoubleArray` into a fresh dense vector. */
        fun of(values: DoubleArray): DenseVector = DenseVector(values.copyOf())

        /** Create a zero vector of length [size]. */
        fun zero(size: Int): DenseVector = DenseVector(size)

        /** Wrap an existing `DoubleArray` without copying. Caller relinquishes ownership. */
        internal fun wrap(data: DoubleArray): DenseVector = DenseVector(data)
    }
}

/**
 * Compressed sparse vector: parallel [indices]/[values] arrays of equal length, each
 * recording one nonzero entry. Always immutable from the caller's perspective — to
 * change the sparsity pattern, rebuild. Linear lookup for [get]; cheap iteration
 * over nonzeros for internal ops (sparsity-aware dot products etc.).
 *
 * Indices are not required to be sorted; the constructor only checks the parallel-
 * array invariant.
 */
@Serializable
@SerialName("SparseVector")
class SparseVector internal constructor(
    override val size: Int,
    internal val indices: IntArray,
    internal val values: DoubleArray,
) : VectorView {

    init {
        require(indices.size == values.size) {
            "indices/values must align: ${indices.size} vs ${values.size}"
        }
    }

    override fun get(i: Int): Double {
        for (k in indices.indices) if (indices[k] == i) return values[k]
        return 0.0
    }

    override fun toDoubleArray(): DoubleArray {
        val out = DoubleArray(size)
        for (k in indices.indices) out[indices[k]] = values[k]
        return out
    }

    override fun equals(other: Any?): Boolean = this === other ||
        (other is SparseVector && size == other.size &&
            indices.contentEquals(other.indices) && values.contentEquals(other.values))
    override fun hashCode(): Int {
        var h = size
        h = 31 * h + indices.contentHashCode()
        h = 31 * h + values.contentHashCode()
        return h
    }
    override fun toString(): String = "SparseVector(size=$size, nnz=${indices.size})"

    companion object {
        /** Build a sparse vector. Copies inputs so the caller can reuse the arrays. */
        fun of(size: Int, indices: IntArray, values: DoubleArray): SparseVector =
            SparseVector(size, indices.copyOf(), values.copyOf())
    }
}
