package com.eignex.kumulant.math

import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class SobolSequenceTest {
    @Test
    fun `unscrambled points agree with independent SciPy direction numbers`() {
        val cases = listOf(
            Triple(0L, 0, 0.0),
            Triple(1L, 1023, 0.5),
            Triple(2L, 31, 0.25),
            Triple(7L, 127, 0.375),
            Triple(8L, 2, 0.9375),
            Triple(31L, 1023, 0.34375),
            Triple(127L, 31, 0.1328125),
            Triple(255L, 127, 0.23828125),
            Triple(1023L, 1023, 0.9951171875),
            Triple(65535L, 1023, 0.4597015380859375),
            Triple(4503599627370495L, 1023, 0.07653089956632919),
        )
        for ((index, coordinate, expected) in cases) {
            val sequence = SobolSequence(1, scrambleSeed = null, dimensionOffset = coordinate)
            sequence.skipTo(index)
            assertEquals(expected, sequence.nextPoint()[0])
        }
    }

    @Test
    fun `scrambled dyadic blocks put one point in every coordinate stratum`() {
        for (seed in listOf(0L, 17L, -1L)) {
            val sequence = SobolSequence(4, seed)
            val bins = Array(4) { IntArray(256) }
            val point = DoubleArray(4)
            repeat(256) {
                sequence.nextPoint(point)
                for (coordinate in point.indices) bins[coordinate][(point[coordinate] * 256).toInt()]++
            }
            for (coordinate in bins) assertTrue(coordinate.all { it == 1 })
        }
    }

    @Test
    fun `projection and sample prefixes remain fixed as dimensions grow`() {
        for (seed in listOf<Long?>(null, 0L, 137L)) {
            val full = SobolSequence(12, seed)
            val prefix = SobolSequence(4, seed)
            val tail = SobolSequence(8, seed, dimensionOffset = 4)
            repeat(32) {
                assertContentEquals(prefix.nextNormal() + tail.nextNormal(), full.nextNormal())
            }
        }
    }

    @Test
    fun `skip and backward reset preserve scrambled sequential positions`() {
        val sequential = SobolSequence(3, 19L)
        val projected = SobolSequence(3, 19L)
        val expected = List(33) { sequential.nextPoint() }
        for (index in listOf(32, 7, 0, 31)) {
            projected.skipTo(index.toLong())
            assertContentEquals(expected[index], projected.nextPoint())
        }
    }

    @Test
    fun `normal blocks have finite values and standard marginal moments`() {
        val sequence = SobolSequence(4, 771L)
        val stats = List(4) { VarianceStat() }
        val point = DoubleArray(4)
        repeat(4096) {
            sequence.nextNormal(point)
            for (coordinate in point.indices) {
                assertTrue(point[coordinate].isFinite())
                stats[coordinate].update(point[coordinate], 0L)
            }
        }
        for (stat in stats) {
            val result = stat.read(0L)
            assertTrue(abs(result.mean) < 0.002)
            assertTrue(abs(result.variance - 1.0) < 0.01)
        }
    }

    @Test
    fun `normal endpoints stay finite without dropping the initial point`() {
        val sequence = SobolSequence(1, scrambleSeed = null)
        assertTrue(sequence.nextNormal()[0].isFinite())
        assertEquals(0.5, sequence.nextPoint()[0])
    }

    @Test
    fun `different scramble seeds change the point set`() {
        assertNotEquals(SobolSequence(1, 1L).nextPoint()[0], SobolSequence(1, 2L).nextPoint()[0])
    }

    @Test
    fun `invalid requests preserve position and the sequence reports exhaustion`() {
        for ((dimensions, offset) in listOf(0 to 0, 1025 to 0, 1 to -1, 2 to 1023)) {
            assertFailsWith<IllegalArgumentException> { SobolSequence(dimensions, dimensionOffset = offset) }
        }
        val sequence = SobolSequence(1, scrambleSeed = null)
        assertFailsWith<IllegalArgumentException> { sequence.nextPoint(DoubleArray(0)) }
        assertEquals(0.0, sequence.nextPoint()[0])
        for (position in listOf(-1L, 1L shl 52)) {
            assertFailsWith<IllegalArgumentException> { sequence.skipTo(position) }
        }
        sequence.skipTo((1L shl 52) - 1)
        assertTrue(sequence.nextNormal()[0].isFinite())
        assertFailsWith<IllegalStateException> { sequence.nextPoint() }
        sequence.skipTo(0)
        assertEquals(0.0, sequence.nextPoint()[0])
    }
}
