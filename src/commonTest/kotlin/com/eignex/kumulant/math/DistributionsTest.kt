package com.eignex.kumulant.math

import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Statistical sanity tests for the random-variate generators. Each test draws ~10k
 * samples and compares sample moments against analytic moments with generous slack.
 * Not a full distributional fit test — just enough to catch sign errors and
 * algorithmic mistakes.
 */
class DistributionsTest {

    @Test
    fun `nextNormal matches mean and variance within slack`() {
        val rng = Random(1)
        val n = 20_000
        var s = 0.0
        var ss = 0.0
        repeat(n) { val x = rng.nextNormal(2.0, 0.5); s += x; ss += x * x }
        val mean = s / n
        val variance = ss / n - mean * mean
        assertTrue(abs(mean - 2.0) < 0.02, "mean=$mean")
        assertTrue(abs(variance - 0.25) < 0.02, "var=$variance")
    }

    @Test
    fun `nextNormal matches higher moments of N(0,1)`() {
        val rng = Random(101)
        val n = 50_000
        var s = 0.0; var ss = 0.0; var s3 = 0.0; var s4 = 0.0
        repeat(n) {
            val x = rng.nextNormal()
            s += x; ss += x * x; s3 += x * x * x; s4 += x * x * x * x
        }
        val mean = s / n
        val variance = ss / n - mean * mean
        val skew = s3 / n
        val kurt = s4 / n
        assertTrue(abs(mean) < 0.02, "mean=$mean")
        assertTrue(abs(variance - 1.0) < 0.03, "var=$variance")
        // N(0,1) has skewness 0, kurtosis 3.
        assertTrue(abs(skew) < 0.05, "skew=$skew")
        assertTrue(abs(kurt - 3.0) < 0.2, "kurt=$kurt")
    }

    @Test
    fun `nextNormal tail produces values beyond Ziggurat R boundary`() {
        // Ziggurat splits its work at R ≈ 3.44; verify the tail-sampling branch
        // actually produces samples beyond it at the analytic rate.
        val rng = Random(3)
        var tailHits = 0
        repeat(100_000) { if (abs(rng.nextNormal()) > 3.5) tailHits++ }
        // P(|N(0,1)| > 3.5) ≈ 4.65e-4 → expect ~46 hits in 100k.
        assertTrue(tailHits in 20..100, "tailHits=$tailHits (expected ~46)")
    }

    @Test
    fun `nextNormal is deterministic given the seed`() {
        val a = Random(123)
        val b = Random(123)
        repeat(50) { assertEquals(a.nextNormal(2.0, 0.5), b.nextNormal(2.0, 0.5)) }
    }

    @Test
    fun `nextGamma alpha=1 fast path matches Exponential moments`() {
        val rng = Random(3)
        val n = 20_000
        var s = 0.0; var ss = 0.0
        repeat(n) { val x = rng.nextGamma(1.0); s += x; ss += x * x }
        val mean = s / n; val variance = ss / n - mean * mean
        // Exp(1) has mean=1, variance=1.
        assertTrue(abs(mean - 1.0) < 0.03, "mean=$mean")
        assertTrue(abs(variance - 1.0) < 0.08, "var=$variance")
    }

    @Test
    fun `nextGamma small integer alpha matches Erlang moments`() {
        val rng = Random(11)
        val n = 20_000
        var s = 0.0; var ss = 0.0
        repeat(n) { val x = rng.nextGamma(3.0); s += x; ss += x * x }
        val mean = s / n; val variance = ss / n - mean * mean
        // Gamma(3, 1) has mean=3, variance=3.
        assertTrue(abs(mean - 3.0) < 0.1, "mean=$mean")
        assertTrue(abs(variance - 3.0) < 0.2, "var=$variance")
    }

    @Test
    fun `nextGamma general alpha works via Marsaglia-Tsang`() {
        val rng = Random(13)
        val n = 20_000
        val alpha = 7.5
        var s = 0.0; var ss = 0.0
        repeat(n) { val x = rng.nextGamma(alpha); s += x; ss += x * x }
        val mean = s / n; val variance = ss / n - mean * mean
        assertTrue(abs(mean - alpha) < 0.15, "mean=$mean")
        assertTrue(abs(variance - alpha) < 0.5, "var=$variance")
    }

    @Test
    fun `nextGamma small alpha works via Stuart boost`() {
        val rng = Random(17)
        val n = 20_000
        val alpha = 0.4
        var s = 0.0; var ss = 0.0
        repeat(n) { val x = rng.nextGamma(alpha); s += x; ss += x * x }
        val mean = s / n; val variance = ss / n - mean * mean
        assertTrue(abs(mean - alpha) < 0.05, "mean=$mean")
        assertTrue(abs(variance - alpha) < 0.1, "var=$variance")
    }

    @Test
    fun `nextBeta uniform fast path is exactly nextDouble`() {
        val a = Random(99)
        val b = Random(99)
        repeat(100) { assertEquals(a.nextDouble(), b.nextBeta(1.0, 1.0)) }
    }

    @Test
    fun `nextBeta power fast paths match power distribution`() {
        val rng = Random(101)
        val n = 20_000
        var s = 0.0
        repeat(n) { s += rng.nextBeta(3.0, 1.0) }
        // Beta(3, 1) has mean = 3 / 4 = 0.75.
        assertTrue(abs(s / n - 0.75) < 0.01, "mean=${s / n}")
    }

    @Test
    fun `nextBeta general matches analytic mean`() {
        val rng = Random(2)
        val n = 20_000
        val a = 2.0; val b = 5.0
        var s = 0.0
        repeat(n) { s += rng.nextBeta(a, b) }
        // Beta(a, b) has mean = a / (a + b).
        val expected = a / (a + b)
        assertTrue(abs(s / n - expected) < 0.01, "mean=${s / n}, expected=$expected")
    }

    @Test
    fun `nextLogNormal rejects bad inputs`() {
        val rng = Random(0)
        assertFailsWith<IllegalArgumentException> { rng.nextLogNormal(0.0, 1.0) }
        assertFailsWith<IllegalArgumentException> { rng.nextLogNormal(-1.0, 1.0) }
        assertFailsWith<IllegalArgumentException> { rng.nextLogNormal(1.0, -1.0) }
    }

    @Test
    fun `nextLogNormal matches analytic mean`() {
        val rng = Random(31)
        val n = 20_000
        val mean = 2.0; val variance = 0.5
        var s = 0.0
        repeat(n) { s += rng.nextLogNormal(mean, variance) }
        assertTrue(abs(s / n - mean) < 0.1, "mean=${s / n}")
    }

    @Test
    fun `nextGamma rejects bad alpha`() {
        val rng = Random(0)
        assertFailsWith<IllegalArgumentException> { rng.nextGamma(0.0) }
        assertFailsWith<IllegalArgumentException> { rng.nextGamma(-1.0) }
    }

    @Test
    fun `nextBeta rejects bad alpha or beta`() {
        val rng = Random(0)
        assertFailsWith<IllegalArgumentException> { rng.nextBeta(0.0, 1.0) }
        assertFailsWith<IllegalArgumentException> { rng.nextBeta(1.0, 0.0) }
    }
}
