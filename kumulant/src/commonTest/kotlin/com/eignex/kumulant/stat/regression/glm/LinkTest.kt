package com.eignex.kumulant.stat.regression.glm

import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
class LinkTest {

    @Test
    fun `Identity loss equals squared error`() {
        val link = Link.Identity
        assertEquals(0.0, link.loss(eta = 1.0, y = 1.0))
        assertEquals(4.0, link.loss(eta = 3.0, y = 1.0))
    }

    @Test
    fun `Logit loss matches softplus form`() {
        val link = Link.Logit
        val eta = 1.5
        val expectedAtOne = ln(1.0 + exp(eta)) - 1.0 * eta
        val expectedAtZero = ln(1.0 + exp(eta))
        assertEquals(expectedAtOne, link.loss(eta, y = 1.0), absoluteTolerance = 1e-12)
        assertEquals(expectedAtZero, link.loss(eta, y = 0.0), absoluteTolerance = 1e-12)
    }

    @Test
    fun `Logit loss is numerically stable at large positive eta`() {
        val link = Link.Logit
        val loss = link.loss(eta = 1000.0, y = 0.0)
        assertTrue(loss.isFinite(), "loss=$loss not finite")
        assertTrue(abs(loss - 1000.0) < 1e-9, "loss = $loss, expected ~1000")
    }

    @Test
    fun `Log loss is exp eta minus y times eta`() {
        val link = Link.Log
        val eta = 0.5
        assertEquals(exp(eta) - 2.0 * eta, link.loss(eta, y = 2.0), absoluteTolerance = 1e-12)
    }
}
