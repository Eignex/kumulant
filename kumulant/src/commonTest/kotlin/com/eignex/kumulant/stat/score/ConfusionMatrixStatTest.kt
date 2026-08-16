package com.eignex.kumulant.stat.score

import com.eignex.kumulant.DELTA
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals

class ConfusionMatrixStatTest {

    @Test
    fun `counts pair into the predicted-by-truth cell`() {
        val s = ConfusionMatrixStat(numClasses = 3)
        s.update(0.0, 0.0)
        s.update(1.0, 1.0)
        s.update(2.0, 2.0)
        s.update(0.0, 1.0)
        s.update(2.0, 0.0)
        val r = s.read()
        assertEquals(1.0, r.count(0, 0), DELTA)
        assertEquals(1.0, r.count(0, 1), DELTA)
        assertEquals(1.0, r.count(1, 1), DELTA)
        assertEquals(1.0, r.count(2, 0), DELTA)
        assertEquals(1.0, r.count(2, 2), DELTA)
        assertEquals(5.0, r.totalWeights, DELTA)
        assertEquals(3.0, r.correct, DELTA)
        assertEquals(3.0 / 5.0, r.accuracy, DELTA)
    }

    @Test
    fun `out of range labels are dropped`() {
        val s = ConfusionMatrixStat(numClasses = 2)
        s.update(5.0, 0.0)
        s.update(0.0, -1.0)
        s.update(Double.NaN, 0.0)
        assertEquals(0.0, s.read().totalWeights, DELTA)
    }

    @Test
    fun `per class precision recall and F1 match formula on a 2-by-2 case`() {
        // truth distribution: 6 positives, 4 negatives.
        // predictions: tp=4, fp=1, fn=2, tn=3.
        val s = ConfusionMatrixStat(numClasses = 2)
        repeat(4) { s.update(1.0, 1.0) } // tp
        repeat(2) { s.update(0.0, 1.0) } // fn
        repeat(1) { s.update(1.0, 0.0) } // fp
        repeat(3) { s.update(0.0, 0.0) } // tn
        val r = s.read()
        // precision_1 = tp / (tp + fp) = 4 / 5; recall_1 = tp / (tp + fn) = 4 / 6.
        assertEquals(4.0 / 5.0, r.precision(1), DELTA)
        assertEquals(4.0 / 6.0, r.recall(1), DELTA)
        val expectedF1 = 2.0 * (4.0 / 5.0) * (4.0 / 6.0) / (4.0 / 5.0 + 4.0 / 6.0)
        assertEquals(expectedF1, r.f1(1), DELTA)
        // precision_0 = 3 / 5; recall_0 = 3 / 4.
        assertEquals(3.0 / 5.0, r.precision(0), DELTA)
        assertEquals(3.0 / 4.0, r.recall(0), DELTA)
        // macro F1 = mean of the two F1s.
        assertEquals((r.f1(0) + r.f1(1)) / 2.0, r.macroF1, DELTA)
        // accuracy = (4 + 3) / 10.
        assertEquals(0.7, r.accuracy, DELTA)
    }

    @Test
    fun `mcc reproduces the binary closed form`() {
        // Same 2x2 case.
        val s = ConfusionMatrixStat(numClasses = 2)
        repeat(4) { s.update(1.0, 1.0) }
        repeat(2) { s.update(0.0, 1.0) }
        repeat(1) { s.update(1.0, 0.0) }
        repeat(3) { s.update(0.0, 0.0) }
        // Binary MCC = (TP*TN - FP*FN) / sqrt((TP+FP)(TP+FN)(TN+FP)(TN+FN)).
        val tp = 4.0
        val tn = 3.0
        val fp = 1.0
        val fn = 2.0
        val expected = (tp * tn - fp * fn) /
            sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
        assertEquals(expected, s.read().mcc, DELTA)
    }

    @Test
    fun `empty stream produces well defined zero accuracy and macro F1`() {
        val r = ConfusionMatrixStat(numClasses = 4).read()
        assertEquals(0.0, r.accuracy, DELTA)
        assertEquals(0.0, r.macroF1, DELTA)
        assertEquals(0.0, r.mcc, DELTA)
    }

    @Test
    fun `merge sums cell weights`() {
        val a = ConfusionMatrixStat(3).apply {
            update(0.0, 0.0)
            update(1.0, 2.0)
        }
        val b = ConfusionMatrixStat(3).apply {
            update(0.0, 0.0)
            update(2.0, 1.0)
        }
        a.merge(b.read())
        val r = a.read()
        assertEquals(2.0, r.count(0, 0), DELTA)
        assertEquals(1.0, r.count(1, 2), DELTA)
        assertEquals(1.0, r.count(2, 1), DELTA)
    }

    @Test
    fun `reset zeroes all cells`() {
        val s = ConfusionMatrixStat(2).apply {
            update(0.0, 1.0)
            update(1.0, 0.0)
        }
        s.reset()
        assertEquals(0.0, s.read().totalWeights, DELTA)
    }
}
