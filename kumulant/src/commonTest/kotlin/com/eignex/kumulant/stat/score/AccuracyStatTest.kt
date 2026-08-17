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
        // This is what changed. Labels were compared on `toLong()`, which truncates, so 1.5 and 1.9 both
        // named class 1 and matched each other - while ConfusionMatrixStat, documented as computing the
        // same accuracy, rejected both observations outright.
        val s = AccuracyStat(numClasses = 3)
        s.update(1.0, 1.0)
        s.update(1.5, 1.9)

        assertEquals(1.0, s.read().mean, DELTA, "a fractional label was scored as a class")
        assertEquals(1.0, s.read().totalWeights, DELTA, "a fractional label was counted as an observation")
    }

    @Test
    fun `a label outside the declared range is ignored`() {
        // The other half of what numClasses buys: the old stat was unbounded, so any label at all counted.
        val s = AccuracyStat(numClasses = 3)
        s.update(1.0, 1.0)
        s.update(7.0, 7.0)
        s.update(-1.0, -1.0)

        assertEquals(1.0, s.read().totalWeights, DELTA, "an out-of-range label was counted")
    }

    @Test
    fun `a NaN label is ignored rather than scored as class zero`() {
        // `NaN.toLong()` is 0, so a NaN prediction against a NaN truth used to score as a correct
        // class-0 prediction. asClassLabel rejects it via the same round-trip that rejects 1.5.
        val s = AccuracyStat(numClasses = 3)
        s.update(0.0, 1.0)
        s.update(Double.NaN, Double.NaN)

        assertEquals(0.0, s.read().mean, DELTA, "a NaN pair was scored as a match")
        assertEquals(1.0, s.read().totalWeights, DELTA, "a NaN pair was counted as an observation")
    }

    @Test
    fun `it agrees with ConfusionMatrixStat on the same stream`() {
        // The invariant the change exists for. ConfusionMatrixResult.accuracy is offered so a caller can
        // cross-check the two, and that was only meaningful on a stream of clean in-range integer labels.
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
    fun `a non-positive class count is refused`() {
        assertFailsWith<IllegalArgumentException> { AccuracyStat(numClasses = 0) }
    }
}
