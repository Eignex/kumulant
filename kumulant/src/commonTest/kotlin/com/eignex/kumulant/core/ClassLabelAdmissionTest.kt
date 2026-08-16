package com.eignex.kumulant.core

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.stat.regression.GaussianNaiveBayesStat
import com.eignex.kumulant.stat.regression.SoftmaxRegressionStat
import com.eignex.kumulant.stat.regression.tree.ClassCountsStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeClassifierStat
import com.eignex.kumulant.stat.regression.tree.RandomForestClassifierStat
import com.eignex.kumulant.stat.regression.tree.ThresholdSplit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val CLASSES = 3
private const val DELTA = 1e-12

/**
 * Which `Double`s name a class, checked on the helper and then on every classifier that uses it.
 *
 * The five call sites disagreed before this: two round-tripped through `Double`, three truncated, and
 * they used three different weight predicates between them. The sweep is the part that matters, because
 * the helper being right does not help if a stat stops calling it.
 */
class ClassLabelAdmissionTest {

    /** One classifier, with a way to feed it and a way to see whether it moved. */
    private class Probe(val name: String, val update: (Double, Double) -> Unit, val snapshot: () -> String)

    private val splits = listOf(ThresholdSplit(featureIndex = 0, threshold = 0.5))
    private val x = DenseVector.of(DoubleArray(2) { 1.0 })

    // Fresh instances per test. Every snapshot reads the learned numbers out explicitly rather than
    // stringifying the result: ClassCountsResult has no toString override, so on the JVM a snapshot
    // taken that way is a fresh identity hash every time and compares unequal to itself, which made an
    // earlier version of the "still absorbed" test pass without proving anything. Kotlin/JS renders
    // those objects differently and failed honestly, which is how it was caught.
    private fun probes(): List<Probe> {
        val softmax = SoftmaxRegressionStat(featureSize = 2, numClasses = CLASSES)
        val gnb = GaussianNaiveBayesStat(featureSize = 2, numClasses = CLASSES)
        val tree = DecisionTreeClassifierStat(2, numClasses = CLASSES, splitCandidates = splits)
        val forest = RandomForestClassifierStat(
            2,
            numClasses = CLASSES,
            splitCandidates = splits,
            nbrTrees = 2,
            bagging = false,
        )
        val counts = ClassCountsStat(CLASSES)
        return listOf(
            Probe("Softmax", { y, w -> softmax.update(x, y, weight = w) }) {
                val r = softmax.read()
                val w = (0 until CLASSES).joinToString { k -> (0 until 2).joinToString { i -> "${r.weights[k, i]}" } }
                "$w|${(0 until CLASSES).joinToString { "${r.biases[it]}" }}|${r.totalWeights}|${r.step}"
            },
            Probe("GaussianNaiveBayes", { y, w -> gnb.update(x, y, weight = w) }) {
                val r = gnb.read()
                val m = (0 until CLASSES).joinToString { c -> (0 until 2).joinToString { i -> "${r.means[c, i]}" } }
                "$m|${(0 until CLASSES).joinToString { "${r.classWeights[it]}" }}|${r.totalWeights}"
            },
            Probe("DecisionTreeClassifier", { y, w -> tree.update(x, y, weight = w) }) {
                tree.tree().rootSnapshot().counts.joinToString()
            },
            Probe("RandomForestClassifier", { y, w -> forest.update(x, y, weight = w) }) {
                forest.trees().joinToString(";") { it.rootSnapshot().counts.joinToString() }
            },
            Probe("ClassCounts", { y, w -> counts.update(y, weight = w) }) {
                counts.read().counts.joinToString()
            },
        )
    }

    @Test
    fun `asClassLabel accepts exactly the integers in range`() {
        for (c in 0 until CLASSES) {
            assertEquals(c, c.toDouble().asClassLabel(CLASSES), "class $c was rejected")
        }
    }

    @Test
    fun `asClassLabel rejects a label that is not an integer`() {
        // The case that used to differ between the tree path and the GLM path: toInt() truncates, so
        // 1.5 arrived as a perfectly ordinary class 1 on three of the five sites.
        for (y in listOf(0.5, 1.5, 2.5, -0.5, 1.0000001, 0.9999999)) {
            assertEquals(-1, y.asClassLabel(CLASSES), "$y was accepted as a class index")
        }
    }

    @Test
    fun `asClassLabel rejects NaN`() {
        // NaN.toInt() is 0, so an unvalidated NaN label is indistinguishable from the first class.
        assertEquals(-1, Double.NaN.asClassLabel(CLASSES))
    }

    @Test
    fun `asClassLabel rejects out-of-range integers and the infinities`() {
        for (y in listOf(-1.0, -2.0, CLASSES.toDouble(), CLASSES + 1.0, 1e18)) {
            assertEquals(-1, y.asClassLabel(CLASSES), "$y was accepted as a class index")
        }
        // toInt() saturates the infinities to Int.MIN/MAX_VALUE, which the round-trip rejects because
        // neither converts back to an infinity.
        assertEquals(-1, Double.POSITIVE_INFINITY.asClassLabel(CLASSES))
        assertEquals(-1, Double.NEGATIVE_INFINITY.asClassLabel(CLASSES))
    }

    @Test
    fun `negative zero is the first class`() {
        // -0.0 == 0.0 under IEEE comparison and the round-trip uses ==, so this is class 0 rather than
        // a rejected negative. Worth pinning because the sign bit makes it look like neither.
        assertEquals(0, (-0.0).asClassLabel(CLASSES))
    }

    @Test
    fun `every classifier refuses a label that is not a class index`() {
        val violations = mutableListOf<String>()
        for (y in listOf(1.5, -0.5, Double.NaN, -1.0, CLASSES.toDouble())) {
            for (probe in probes()) {
                val before = probe.snapshot()

                probe.update(y, 1.0)

                if (probe.snapshot() != before) violations += "${probe.name} absorbed the label $y"
            }
        }
        assertEquals(emptyList(), violations.toList(), "a label that is not a class index must be dropped")
    }

    @Test
    fun `every classifier treats a NaN weight as a no-op`() {
        // The half of this that was a live defect rather than an inconsistency. `weight <= 0.0` and
        // `weight == 0.0` are both false for NaN, so on three of the five paths a NaN weight reached
        // the leaf counts and pinned one to NaN permanently: a later valid observation could not clear
        // it, because every subsequent add started from NaN.
        val violations = mutableListOf<String>()
        for (probe in probes()) {
            probe.update(1.0, 1.0)
            val before = probe.snapshot()

            probe.update(1.0, Double.NaN)

            if (probe.snapshot() != before) violations += "${probe.name} absorbed a NaN weight"
            if ("NaN" in probe.snapshot()) violations += "${probe.name} is now holding a NaN"

            // And it still works afterwards, which is the part a state comparison alone would miss.
            probe.update(2.0, 1.0)
            if ("NaN" in probe.snapshot()) violations += "${probe.name} was poisoned by the NaN weight"
        }
        assertEquals(emptyList(), violations.toList(), "a NaN weight must not reach any classifier's state")
    }

    @Test
    fun `a valid label at a live weight is still absorbed`() {
        // Guards the guards above: a stat that rejected everything would satisfy all of them.
        for (probe in probes()) {
            val before = probe.snapshot()

            probe.update(1.0, 1.0)

            assertTrue(probe.snapshot() != before, "${probe.name} ignored a valid label at weight 1.0")
        }
    }

    @Test
    fun `a class count still downdates on a negative weight`() {
        // ClassCounts guards on isInertWeight rather than isNotPositiveWeight, because a count cell
        // subtracts exactly: retracting an observation folded in earlier is a downdate, not corruption.
        // The regression trees already worked this way and the classification side now matches, so the
        // fix to the NaN-weight hole did not cost the legitimate negative-weight case.
        val counts = ClassCountsStat(CLASSES)
        counts.update(1.0, weight = 3.0)
        counts.update(1.0, weight = -1.0)

        assertEquals(2.0, counts.read().counts[1], DELTA, "the downdate did not subtract")
    }
}
