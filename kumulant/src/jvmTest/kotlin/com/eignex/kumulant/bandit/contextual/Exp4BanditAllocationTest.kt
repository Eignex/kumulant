package com.eignex.kumulant.bandit.contextual

import com.eignex.koblas.core.F64DenseVector
import com.eignex.kumulant.assertAllocatesAtMost
import com.eignex.kumulant.bytesPerCall
import kotlin.test.Test
import kotlin.test.assertTrue

class Exp4BanditAllocationTest {

    @Test
    fun `destination distribution removes Kumulant output allocation with reusable advice`() {
        val allocating = reusableBandit()
        val destination = reusableBandit()
        val x = F64DenseVector.of(doubleArrayOf(1.0))
        val out = DoubleArray(ARMS)

        val (allocatingBytes, destinationBytes) = bytesPerCall(
            { allocating.playDistribution(x) },
            { destination.playDistributionInto(x, out) },
        )

        assertTrue(
            destinationBytes + 128.0 <= allocatingBytes,
            "destination distribution allocated $destinationBytes B/call versus $allocatingBytes B/call",
        )
    }

    @Test
    fun `choose and update retain their distribution storage with reusable advice`() {
        val x = F64DenseVector.of(doubleArrayOf(1.0))
        val choosing = reusableBandit()
        val updating = reusableBandit()
        val arm = updating.choose(x)

        assertAllocatesAtMost(0.0, "choose") { choosing.choose(x) }
        assertAllocatesAtMost(0.0, "update followed by choose") {
            updating.update(arm, x, reward = 0.0)
            updating.choose(x)
        }
    }

    private fun reusableBandit(): Exp4Bandit {
        val advice = Array(EXPERTS) { expert -> DoubleArray(ARMS) { arm -> (expert + arm + 1).toDouble() } }
        return Exp4Bandit(
            nbrArms = ARMS,
            experts = advice.map { values -> Exp4Expert { _, _ -> values } },
            gamma = 0.1,
        )
    }

    private companion object {
        const val EXPERTS = 8
        const val ARMS = 32
    }
}
