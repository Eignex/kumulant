package com.eignex.kumulant.core

import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Accuracy
import com.eignex.kumulant.schema.spec.Auc
import com.eignex.kumulant.schema.spec.BloomFilter
import com.eignex.kumulant.schema.spec.BrierScore
import com.eignex.kumulant.schema.spec.ConfusionMatrix
import com.eignex.kumulant.schema.spec.CountMinSketch
import com.eignex.kumulant.schema.spec.Covariance
import com.eignex.kumulant.schema.spec.DiscreteStatSpec
import com.eignex.kumulant.schema.spec.HyperLogLog
import com.eignex.kumulant.schema.spec.IsotonicCalibrator
import com.eignex.kumulant.schema.spec.LinearCounting
import com.eignex.kumulant.schema.spec.LogLoss
import com.eignex.kumulant.schema.spec.MaeLoss
import com.eignex.kumulant.schema.spec.MinHash
import com.eignex.kumulant.schema.spec.MseLoss
import com.eignex.kumulant.schema.spec.PairedStatSpec
import com.eignex.kumulant.schema.spec.PinballLoss
import com.eignex.kumulant.schema.spec.PlattCalibrator
import com.eignex.kumulant.schema.spec.Reliability
import com.eignex.kumulant.schema.spec.Sojourn
import com.eignex.kumulant.schema.spec.SpaceSaving
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The same library-wide contracts as [SeriesStatContractSweepTest], over the paired and discrete
 * modalities. [Stat] states the zero-weight guarantee holds "whatever the modality", so the sweep
 * has to cross modalities to be worth anything.
 */
class PairedAndDiscreteContractSweepTest {

    private val pairedSpecs: List<Pair<String, PairedStatSpec<*>>> = listOf(
        "Covariance" to Covariance,
        "BrierScore" to BrierScore,
        "MseLoss" to MseLoss,
        "MaeLoss" to MaeLoss,
        "LogLoss" to LogLoss,
        "PinballLoss" to PinballLoss(tau = 0.5),
        "Auc" to Auc(),
        "Reliability" to Reliability(numBins = 8),
        "PlattCalibrator" to PlattCalibrator(),
        "IsotonicCalibrator" to IsotonicCalibrator(),
        "ConfusionMatrix" to ConfusionMatrix(numClasses = 3),
        "Accuracy" to Accuracy,
    )

    private val discreteSpecs: List<Pair<String, DiscreteStatSpec<*>>> = listOf(
        // HyperLogLog was missing from this catalogue, which is how it stayed the one sketch no sweep
        // covered even though it carries the same weight guard as the other five.
        "HyperLogLog" to HyperLogLog(),
        "LinearCounting" to LinearCounting(),
        "BloomFilter" to BloomFilter(),
        "CountMinSketch" to CountMinSketch(),
        "MinHash" to MinHash(),
        "Sojourn" to Sojourn(states = listOf(0L, 1L, 2L)),
        "SpaceSaving" to SpaceSaving(capacity = 8),
    )

    // Values inside the unit interval so the probability-shaped stats (log loss, calibration,
    // Brier, AUC, confusion) all see legal input.
    private val pairs = listOf(0.1 to 0.0, 0.9 to 1.0, 0.4 to 0.0, 0.75 to 1.0, 0.55 to 1.0, 0.2 to 0.0)
    private val keys = longArrayOf(0L, 1L, 2L, 1L, 0L, 2L, 1L)
    private val readAt = 8_000_000_000L

    @Test
    fun `a zero weight is a no-op for every paired stat`() {
        val violations = mutableListOf<String>()
        for ((name, spec) in pairedSpecs) {
            for (probe in listOf(0.99 to 0.0, 0.01 to 1.0)) {
                val stat = spec.materialize()
                pairs.forEachIndexed { i, (x, y) -> stat.update(x, y, i.toLong() * 1_000_000_000L, 1.0) }
                val before = stat.read(readAt)

                stat.update(probe.first, probe.second, readAt, 0.0)

                val after = stat.read(readAt)
                if (before != after) violations += "$name absorbed a zero-weight $probe: $before -> $after"
            }
        }
        assertEquals(emptyList(), violations.toList(), "zero-weight updates must not change state")
    }

    @Test
    fun `a zero weight is a no-op for every discrete stat`() {
        val violations = mutableListOf<String>()
        for ((name, spec) in discreteSpecs) {
            val stat = spec.materialize()
            keys.forEachIndexed { i, k -> stat.update(k, i.toLong() * 1_000_000_000L, 1.0) }
            val before = stat.read(readAt)

            // A key the stat already knows, so this probes weight handling and not key validation.
            stat.update(2L, readAt, 0.0)

            val after = stat.read(readAt)
            if (before != after) violations += "$name absorbed a zero-weight key: $before -> $after"
        }
        assertEquals(emptyList(), violations.toList(), "zero-weight updates must not change state")
    }

    @Test
    fun `a NaN weight is a no-op for every paired and discrete stat`() {
        val violations = mutableListOf<String>()
        for ((name, spec) in pairedSpecs) {
            val stat = spec.materialize()
            pairs.forEachIndexed { i, (x, y) -> stat.update(x, y, i.toLong() * 1_000_000_000L, 1.0) }
            val before = stat.read(readAt)

            val thrown = runCatching { stat.update(0.99, 0.0, readAt, Double.NaN) }.exceptionOrNull()
            if (thrown != null) {
                violations += "$name threw on a NaN weight: ${thrown.message}"
                continue
            }

            val after = stat.read(readAt)
            if (before != after) violations += "$name absorbed a NaN weight: $before -> $after"
        }
        for ((name, spec) in discreteSpecs) {
            val stat = spec.materialize()
            keys.forEachIndexed { i, k -> stat.update(k, i.toLong() * 1_000_000_000L, 1.0) }
            val before = stat.read(readAt)

            // The sketches were the worst case here: `weight <= 0.0` is false for NaN and
            // `ceil(NaN).toLong()` is 0, which the coerce lifted to 1, so a NaN weight became a real
            // observation of weight one.
            val thrown = runCatching { stat.update(2L, readAt, Double.NaN) }.exceptionOrNull()
            if (thrown != null) {
                violations += "$name threw on a NaN weight: ${thrown.message}"
                continue
            }

            val after = stat.read(readAt)
            if (before != after) violations += "$name absorbed a NaN weight: $before -> $after"
        }
        assertEquals(emptyList(), violations.map { it.take(110) }, "a NaN weight must not change state")
    }

    @Test
    fun `an infinite weight is a no-op for every paired and discrete stat`() {
        // The paired modality was the worst of the three here: nine of its twelve stats reported NaN
        // after a `+Infinity` weight, because a loss or a calibration curve divides an accumulated total
        // by an accumulated weight and both had gone infinite. See isInertWeight on why an infinity is
        // not a multiplicity and why the limit was not worth chasing.
        val violations = mutableListOf<String>()
        for (weight in doubleArrayOf(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            for ((name, spec) in pairedSpecs) {
                val stat = spec.materialize()
                pairs.forEachIndexed { i, (x, y) -> stat.update(x, y, i.toLong() * 1_000_000_000L, 1.0) }
                val before = stat.read(readAt)

                val thrown = runCatching { stat.update(0.99, 0.0, readAt, weight) }.exceptionOrNull()
                if (thrown != null) {
                    violations += "$name threw on a weight of $weight: ${thrown.message}"
                    continue
                }

                if (stat.read(readAt) != before) violations += "$name absorbed a $weight weight"
            }
            for ((name, spec) in discreteSpecs) {
                val stat = spec.materialize()
                keys.forEachIndexed { i, k -> stat.update(k, i.toLong() * 1_000_000_000L, 1.0) }
                val before = stat.read(readAt)

                val thrown = runCatching { stat.update(2L, readAt, weight) }.exceptionOrNull()
                if (thrown != null) {
                    violations += "$name threw on a weight of $weight: ${thrown.message}"
                    continue
                }

                if (stat.read(readAt) != before) violations += "$name absorbed a $weight weight"
            }
        }
        assertEquals(emptyList(), violations.map { it.take(110) }, "an infinite weight must not change state")
    }

    @Test
    fun `a negative weight is a no-op for every discrete sketch`() {
        // The discrete modality is entirely no-inverse: there is no bucket decrement that undoes a
        // hash insert into a Bloom filter or an HLL register, so these all drop a negative weight
        // rather than downdating it. Sojourn is the one exception in the catalogue - it accumulates
        // residence time additively and subtracts, so it guards on isInertWeight like the series
        // accumulators. See isNotPositiveWeight for why the two predicates are named separately.
        val violations = mutableListOf<String>()
        for ((name, spec) in discreteSpecs) {
            if (name in DOWNDATES) continue

            // A key none of the primed updates used, so a live-weight probe visibly moves every
            // sketch here; without that the negative-weight assertion below would be vacuous.
            val absorbing = spec.materialize()
            keys.forEachIndexed { i, k -> absorbing.update(k, i.toLong() * 1_000_000_000L, 1.0) }
            val baseline = absorbing.read(readAt)
            absorbing.update(PROBE_KEY, readAt, 1.0)
            if (absorbing.read(readAt) == baseline) {
                violations += "$name ignored a positive-weight key, so the negative case proves nothing"
                continue
            }

            val stat = spec.materialize()
            keys.forEachIndexed { i, k -> stat.update(k, i.toLong() * 1_000_000_000L, 1.0) }
            val before = stat.read(readAt)

            val thrown = runCatching { stat.update(PROBE_KEY, readAt, -1.0) }.exceptionOrNull()
            if (thrown != null) {
                violations += "$name threw on a negative weight: ${thrown.message}"
                continue
            }

            val after = stat.read(readAt)
            if (before != after) violations += "$name absorbed a negative-weight key: $before -> $after"
        }
        assertEquals(emptyList(), violations.map { it.take(110) }, "a sketch must drop a negative weight")
    }

    @Test
    fun `no paired stat throws on a NaN value`() {
        // Both positions separately: a paired stat reaches x and y through different arithmetic, so
        // one being safe is no evidence about the other. A NaN value propagates rather than being
        // filtered; the only guarantee is that it does not become an exception.
        val violations = mutableListOf<String>()
        for ((name, spec) in pairedSpecs) {
            for (probe in listOf(Double.NaN to 1.0, 0.5 to Double.NaN, Double.NaN to Double.NaN)) {
                val stat = spec.materialize()
                pairs.forEachIndexed { i, (x, y) -> stat.update(x, y, i.toLong() * 1_000_000_000L, 1.0) }

                val thrown = runCatching { stat.update(probe.first, probe.second, readAt, 1.0) }.exceptionOrNull()
                if (thrown != null) violations += "$name threw on a NaN $probe: ${thrown.message}"
                runCatching { stat.read(readAt) }.exceptionOrNull()?.let {
                    violations += "$name threw reading back after a NaN $probe: ${it.message}"
                }
            }
        }
        assertEquals(emptyList(), violations.map { it.take(110) }, "a NaN value must never throw")
    }

    @Test
    fun `every result equals itself`() {
        // Reflexivity is not free here: several Results hand-roll equals to get contentEquals on
        // their arrays, and a hand-rolled `a == other.a` on a Double is IEEE comparison, so a field
        // that is legitimately NaN makes the result unequal to itself. The generated data-class
        // equals would not have this problem.
        val violations = mutableListOf<String>()
        for ((name, spec) in pairedSpecs) {
            val fresh = spec.materialize()
            if (fresh.read(readAt) != fresh.read(readAt)) violations += "$name (empty): ${fresh.read(readAt)}"
            pairs.forEachIndexed { i, (x, y) -> fresh.update(x, y, i.toLong() * 1_000_000_000L, 1.0) }
            if (fresh.read(readAt) != fresh.read(readAt)) violations += "$name (primed): ${fresh.read(readAt)}"
        }
        for ((name, spec) in discreteSpecs) {
            val fresh = spec.materialize()
            if (fresh.read(readAt) != fresh.read(readAt)) violations += "$name (empty): ${fresh.read(readAt)}"
            keys.forEachIndexed { i, k -> fresh.update(k, i.toLong() * 1_000_000_000L, 1.0) }
            if (fresh.read(readAt) != fresh.read(readAt)) violations += "$name (primed): ${fresh.read(readAt)}"
        }
        assertEquals(emptyList(), violations.map { it.take(80) }, "a result must equal itself")
    }

    @Test
    fun `reset returns every paired and discrete stat to its fresh state`() {
        val violations = mutableListOf<String>()
        for ((name, spec) in pairedSpecs) {
            val stat = spec.materialize()
            pairs.forEachIndexed { i, (x, y) -> stat.update(x, y, i.toLong() * 1_000_000_000L, 1.0) }
            stat.reset()
            val fresh = spec.materialize().read(readAt)
            if (fresh != stat.read(readAt)) violations += "$name: ${stat.read(readAt)} != fresh $fresh"
        }
        for ((name, spec) in discreteSpecs) {
            val stat = spec.materialize()
            keys.forEachIndexed { i, k -> stat.update(k, i.toLong() * 1_000_000_000L, 1.0) }
            stat.reset()
            val fresh = spec.materialize().read(readAt)
            if (fresh != stat.read(readAt)) violations += "$name: ${stat.read(readAt)} != fresh $fresh"
        }
        assertEquals(emptyList(), violations.toList(), "reset must restore the fresh state")
    }

    private companion object {
        /**
         * The one discrete stat whose accumulation inverts, so a negative weight subtracts rather than
         * being dropped. Everything else in the catalogue is a hash sketch with no decrement.
         */
        val DOWNDATES = setOf("Sojourn")

        /** A key none of the primed updates touch, so a live-weight probe moves every sketch. */
        const val PROBE_KEY = 97L
    }
}
