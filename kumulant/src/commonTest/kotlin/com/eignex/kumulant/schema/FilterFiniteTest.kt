package com.eignex.kumulant.schema

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.isFinite
import com.eignex.kumulant.schema.expr.isNaN
import com.eignex.kumulant.schema.expr.not
import com.eignex.kumulant.schema.ops.filter
import com.eignex.kumulant.schema.ops.filterFinite
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Covariance
import com.eignex.kumulant.schema.spec.Mean
import com.eignex.kumulant.schema.spec.StatSpec
import com.eignex.kumulant.schema.spec.StochasticRegression
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.skema.SchemaJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private val NON_FINITE = doubleArrayOf(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)

/**
 * `filterFinite`, the supported way to keep non-finite observations out of a stat.
 *
 * [Stat][com.eignex.kumulant.core.Stat] guarantees only that a non-finite value will not throw. It is
 * explicitly allowed to poison the stat, and for an accumulator it does: a `NaN` folded into a mean is
 * reported for ever. That is the right default, because the observation genuinely arrived and genuinely
 * was unusable, and hiding it would turn a gap in the input into a silently plausible answer. A caller
 * who would rather drop such observations says so with this filter.
 */
class FilterFiniteTest {

    @Test
    fun `a series stat behind the filter never sees a non-finite value`() {
        for (bad in NON_FINITE) {
            val filtered = Mean.filterFinite().materialize()
            filtered.update(4.0)
            filtered.update(bad)
            filtered.update(6.0)

            val r = filtered.read()
            assertEquals(5.0, r.mean, DELTA, "the mean absorbed $bad")
            assertEquals(2.0, r.totalWeights, DELTA, "$bad was counted as an observation")
        }
    }

    @Test
    fun `without the filter the same value propagates as documented`() {
        // The other half of the contract, pinned so the filter's purpose stays legible. This is not a
        // defect being tolerated: it is the stat reporting that it was given something unusable.
        val plain = Mean.materialize()
        plain.update(4.0)
        plain.update(Double.NaN)
        plain.update(6.0)

        assertTrue(plain.read().mean.isNaN(), "a NaN value should propagate when nothing filters it")
    }

    @Test
    fun `a paired stat behind the filter rejects a non-finite coordinate on either side`() {
        for (bad in NON_FINITE) {
            val onX = Covariance.filterFinite().materialize()
            onX.update(1.0, 1.0)
            onX.update(bad, 1.0)
            onX.update(3.0, 3.0)
            assertEquals(2.0, onX.read().totalWeights, DELTA, "a non-finite x ($bad) was absorbed")

            val onY = Covariance.filterFinite().materialize()
            onY.update(1.0, 1.0)
            onY.update(1.0, bad)
            onY.update(3.0, 3.0)
            assertEquals(2.0, onY.read().totalWeights, DELTA, "a non-finite y ($bad) was absorbed")
        }
    }

    @Test
    fun `a regression stat behind the filter rejects a non-finite feature or target`() {
        // The modality where this matters most, because a poisoned coefficient never recovers: every
        // later gradient step multiplies the NaN forward.
        for (bad in NON_FINITE) {
            val good = DenseVector.of(doubleArrayOf(1.0, 1.0))

            val onFeature = StochasticRegression(featureSize = 2).filterFinite().materialize()
            onFeature.update(good, 1.0)
            onFeature.update(DenseVector.of(doubleArrayOf(bad, 1.0)), 1.0)
            onFeature.update(good, 1.0)
            val f = onFeature.read()
            assertEquals(2.0, f.totalWeights, DELTA, "a non-finite feature ($bad) was absorbed")
            assertTrue(f.weights[0].isFinite(), "a non-finite feature ($bad) reached the coefficients")

            val onTarget = StochasticRegression(featureSize = 2).filterFinite().materialize()
            onTarget.update(good, 1.0)
            onTarget.update(good, bad)
            onTarget.update(good, 1.0)
            val t = onTarget.read()
            assertEquals(2.0, t.totalWeights, DELTA, "a non-finite target ($bad) was absorbed")
            assertTrue(t.weights[0].isFinite(), "a non-finite target ($bad) reached the coefficients")
        }
    }

    @Test
    fun `the filter rejects a non-finite coordinate anywhere in a feature vector`() {
        // What no per-coordinate expression can express, and the reason AllFinite exists as its own node
        // rather than as sugar over V(0).isFinite(): the bad coordinate can be any of them.
        for (index in 0 until 4) {
            val stat = StochasticRegression(featureSize = 4).filterFinite().materialize()
            stat.update(DenseVector.of(DoubleArray(4) { 1.0 }), 1.0)

            val poisoned = DoubleArray(4) { if (it == index) Double.NaN else 1.0 }
            stat.update(DenseVector.of(poisoned), 1.0)

            assertEquals(1.0, stat.read().totalWeights, DELTA, "a NaN at coordinate $index slipped through")
        }
    }

    @Test
    fun `a finite observation still passes the filter`() {
        // Guards every assertion above: a filter that rejected everything would satisfy all of them.
        val stat = Sum.filterFinite().materialize()
        stat.update(3.0)
        stat.update(4.0)

        assertEquals(7.0, stat.read().sum, DELTA, "the filter dropped a perfectly good observation")
    }

    @Test
    fun `filtering on finiteness is stricter than filtering on NaN`() {
        // The distinction that justifies a second predicate. An infinity is not a NaN, so `!isNaN()`
        // lets it through, and an infinity in a mean is just as permanent. Note it leaves the mean at
        // +Infinity rather than NaN here, which is worse in a way: it still looks like a bound.
        val nanOnly = Mean.filter(!X.isNaN()).materialize()
        nanOnly.update(4.0)
        nanOnly.update(Double.POSITIVE_INFINITY)
        assertTrue(!nanOnly.read().mean.isFinite(), "an infinity got past the NaN filter and then broke the mean")

        val finite = Mean.filter(X.isFinite()).materialize()
        finite.update(4.0)
        finite.update(Double.POSITIVE_INFINITY)
        assertEquals(4.0, finite.read().mean, DELTA, "the finiteness filter should have dropped the infinity")
    }

    @Test
    fun `the filter survives a round trip through the wire`() {
        // Filters are required to be wire-expressible; a predicate that only worked in-process would be
        // unusable from a serialised pipeline, which is the point of putting it in the AST.
        val spec: StatSpec = Mean.filterFinite() as StatSpec
        val json = SchemaJson.encodeToString(StatSpec.serializer(), spec)
        assertTrue("AllFinite" in json, "the predicate did not reach the wire: $json")

        val decoded = SchemaJson.decodeFromString(StatSpec.serializer(), json)
        assertEquals(spec, decoded, "the spec did not survive the round trip")

        val revived = (decoded as com.eignex.kumulant.schema.spec.SeriesStatSpec<*>).materialize()
        revived.update(4.0)
        revived.update(Double.NaN)
        revived.update(6.0)
        assertTrue("NaN" !in revived.read().toString(), "the decoded filter stopped filtering")
    }
}
