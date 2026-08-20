package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals

class StreamModeZeroAddTest {

    // HighWrite is excluded on purpose: a striped adder sums from a +0.0 base, so it cannot represent a
    // negative zero at all and loses the sign when the cell is seeded, before any add. See AdderMode.
    private val levelsThatCanSignZero = Concurrency.entries.filter { it != Concurrency.HighWrite }

    @Test
    fun `adding zero leaves the sign of a negative zero alone at every level`() {
        for (level in levelsThatCanSignZero) {
            val cell = level.additiveMode().newDouble(-0.0)
            cell.add(0.0)
            assertEquals(
                (-0.0).toRawBits(),
                cell.load().toRawBits(),
                "level=$level flipped the sign of a reported zero",
            )
        }
    }

    @Test
    fun `adding zero to an array cell leaves the sign alone at every level`() {
        for (level in levelsThatCanSignZero) {
            val cells = level.additiveMode().newDoubleArray(1) { -0.0 }
            cells.add(0, 0.0)
            assertEquals(
                (-0.0).toRawBits(),
                cells.load(0).toRawBits(),
                "level=$level flipped the sign of a reported zero",
            )
        }
    }
}
