package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.ReservoirResult
import com.eignex.kumulant.core.SeriesStat
import kotlin.math.pow
import kotlin.random.Random

/**
 * Weighted reservoir sample of size [capacity] via Algorithm A-Res
 * (Efraimidis & Spirakis): each item gets a key `u^(1/w)` and the top-`k`
 * keys are retained, giving an unbiased weight-proportional sample.
 *
 * State is held behind a CAS-swapped reference; under [com.eignex.kumulant.concurrent.AtomicMode]
 * concurrent updates are racy but eventually consistent. For strict
 * thread-safety, wrap in `.locked()`.
 */
class ReservoirHistogram(
    val capacity: Int = 1024,
    val seed: Long = 0L,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<ReservoirResult> {

    init {
        require(capacity > 0) { "capacity must be > 0" }
    }

    private class State(
        val values: DoubleArray,
        val keys: DoubleArray,
        val totalSeen: Long,
        val totalWeight: Double
    )

    private val random = Random(seed)
    private val stateRef = mode.newReference(
        State(DoubleArray(0), DoubleArray(0), 0L, 0.0)
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0 || value.isNaN()) return

        val u = random.nextDouble()
        val key = if (weight == 1.0) u else u.pow(1.0 / weight)

        while (true) {
            val s = stateRef.load()
            val newState: State
            if (s.values.size < capacity) {
                val newVals = s.values.copyOf(s.values.size + 1)
                val newKeys = s.keys.copyOf(s.keys.size + 1)
                newVals[s.values.size] = value
                newKeys[s.keys.size] = key
                newState = State(newVals, newKeys, s.totalSeen + 1, s.totalWeight + weight)
            } else {
                var minIdx = 0
                var minKey = s.keys[0]
                for (i in 1 until s.keys.size) {
                    if (s.keys[i] < minKey) {
                        minKey = s.keys[i]
                        minIdx = i
                    }
                }
                if (key > minKey) {
                    val newVals = s.values.copyOf()
                    val newKeys = s.keys.copyOf()
                    newVals[minIdx] = value
                    newKeys[minIdx] = key
                    newState = State(newVals, newKeys, s.totalSeen + 1, s.totalWeight + weight)
                } else {
                    newState = State(s.values, s.keys, s.totalSeen + 1, s.totalWeight + weight)
                }
            }
            if (stateRef.compareAndSet(s, newState)) return
        }
    }

    override fun create(mode: StreamMode?) = ReservoirHistogram(
        capacity,
        seed,
        mode ?: this.mode
    )

    override fun merge(values: ReservoirResult) {
        require(values.values.size == values.keys.size) {
            "ReservoirResult values/keys size mismatch"
        }
        while (true) {
            val s = stateRef.load()
            val combinedVals = s.values + values.values
            val combinedKeys = s.keys + values.keys

            val (vOut, kOut) = if (combinedVals.size <= capacity) {
                combinedVals to combinedKeys
            } else {
                val indices = (combinedKeys.indices).sortedByDescending { combinedKeys[it] }
                    .take(capacity)
                val v = DoubleArray(capacity) { combinedVals[indices[it]] }
                val k = DoubleArray(capacity) { combinedKeys[indices[it]] }
                v to k
            }
            val newState = State(
                vOut,
                kOut,
                s.totalSeen + values.totalSeen,
                s.totalWeight + values.totalWeight
            )
            if (stateRef.compareAndSet(s, newState)) return
        }
    }

    override fun reset() {
        stateRef.store(State(DoubleArray(0), DoubleArray(0), 0L, 0.0))
    }

    override fun read(timestampNanos: Long): ReservoirResult {
        val s = stateRef.load()
        return ReservoirResult(
            values = s.values.copyOf(),
            keys = s.keys.copyOf(),
            capacity = capacity,
            totalSeen = s.totalSeen,
            totalWeight = s.totalWeight
        )
    }
}
