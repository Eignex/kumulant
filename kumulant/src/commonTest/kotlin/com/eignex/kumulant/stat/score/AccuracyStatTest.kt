package com.eignex.kumulant.stat.score

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class AccuracyStatTest {

    @Test
    fun `accuracy is the fraction of matching class labels`() {
        val s = AccuracyStat(numClasses = 3)
        s.update(1.0, 1.0)
        s.update(0.0, 0.0)
        s.update(1.0, 0.0)
        s.update(2.0, 2.0)
        s.update(0.0, 1.0)
        // 3 out of 5 match.
        assertEquals(3.0 / 5.0, s.read().mean, DELTA)
    }

    @Test
    fun `integer doubles name the class they look like`() {
        val s = AccuracyStat(numClasses = 8)
        s.update(7.0, 7.0)
        s.update(2.0, 3.0)
        assertEquals(0.5, s.read().mean, DELTA)
    }

    @Test
    fun `a non-integral label is not a class and is ignored`() {
        // A truncating comparison would make 1.5 and 1.9 both name class 1 and match each other.
        val s = AccuracyStat(numClasses = 3)
        s.update(1.0, 1.0)
        s.update(1.5, 1.9)

        assertEquals(1.0, s.read().mean, DELTA, "a fractional label was scored as a class")
        assertEquals(1.0, s.read().totalWeights, DELTA, "a fractional label was counted as an observation")
    }

    @Test
    fun `a label outside the declared range is ignored`() {
        // The other half of what numClasses buys: an unbounded stat would count any label at all.
        val s = AccuracyStat(numClasses = 3)
        s.update(1.0, 1.0)
        s.update(7.0, 7.0)
        s.update(-1.0, -1.0)

        assertEquals(1.0, s.read().totalWeights, DELTA, "an out-of-range label was counted")
    }

    @Test
    fun `a NaN label is ignored rather than scored as class zero`() {
        // `NaN.toLong()` is 0, so a truncating comparison would score a NaN pair as a correct class-0
        // prediction. asClassLabel rejects it via the same round-trip that rejects 1.5.
        val s = AccuracyStat(numClasses = 3)
        s.update(0.0, 1.0)
        s.update(Double.NaN, Double.NaN)

        assertEquals(0.0, s.read().mean, DELTA, "a NaN pair was scored as a match")
        assertEquals(1.0, s.read().totalWeights, DELTA, "a NaN pair was counted as an observation")
    }

    @Test
    fun `it agrees with ConfusionMatrixStat on the same stream`() {
        // ConfusionMatrixResult.accuracy is offered so a caller can cross-check the two, which requires
        // that they agree on which observations count, not merely on how they are counted.
        val accuracy = AccuracyStat(numClasses = 3)
        val matrix = ConfusionMatrixStat(numClasses = 3)
        val pairs = listOf(
            1.0 to 1.0, 0.0 to 0.0, 1.0 to 0.0, 2.0 to 2.0, 0.0 to 1.0,
            1.5 to 1.9, 7.0 to 7.0, -1.0 to 0.0, Double.NaN to 1.0, 2.0 to Double.NaN,
        )
        for ((p, t) in pairs) {
            accuracy.update(p, t)
            matrix.update(p, t)
        }

        assertEquals(matrix.read().accuracy, accuracy.read().mean, DELTA, "the two disagree on accuracy")
    }

    @Test
    fun `a class count below two is refused`() {
        // One class is not a classification problem, and the counters used to accept it while every
        // model required two. A binary problem is `numClasses = 2`.
        assertFailsWith<IllegalArgumentException> { AccuracyStat(numClasses = 0) }
        assertFailsWith<IllegalArgumentException> { AccuracyStat(numClasses = 1) }
    }
}
