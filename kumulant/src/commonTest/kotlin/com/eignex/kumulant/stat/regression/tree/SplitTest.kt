package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.feat
import com.eignex.kumulant.schema.expr.V
import com.eignex.kumulant.schema.expr.X
import com.eignex.kumulant.schema.expr.gt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SplitTest {


    @Test
    fun `ThresholdSplit routes by inclusive less-than-or-equal`() {
        val s = ThresholdSplit(featureIndex = 0, threshold = 0.5)
        assertTrue(s.direction(feat(0.3)))
        assertTrue(s.direction(feat(0.5)))
        assertFalse(s.direction(feat(0.7)))
    }

    @Test
    fun `ThresholdSplit reads only the configured feature index`() {
        val s = ThresholdSplit(featureIndex = 1, threshold = 0.0)
        assertTrue(s.direction(feat(99.0, -1.0)))
        assertFalse(s.direction(feat(-99.0, 1.0)))
    }

    @Test
    fun `ThresholdSplit toString reflects predicate`() {
        assertEquals("x[2] <= 1.5", ThresholdSplit(2, 1.5).toString())
    }

    @Test
    fun `ThresholdSplit equality is structural`() {
        assertEquals(ThresholdSplit(0, 0.5), ThresholdSplit(0, 0.5))
        assertTrue(ThresholdSplit(0, 0.5) != ThresholdSplit(1, 0.5))
        assertTrue(ThresholdSplit(0, 0.5) != ThresholdSplit(0, 0.6))
    }

    @Test
    fun `ExprSplit routes by BoolExpr against context`() {
        val s = ExprSplit(X gt 0.0)
        assertTrue(s.direction(feat(1.0)))
        assertFalse(s.direction(feat(-1.0)))
    }

    @Test
    fun `ExprSplit can read arbitrary indices via V`() {
        val s = ExprSplit(V(2) gt 0.0)
        assertTrue(s.direction(feat(-1.0, -1.0, 0.5)))
        assertFalse(s.direction(feat(1.0, 1.0, -0.5)))
    }
}
