package com.eignex.kumulant.bandit.contextual

import com.eignex.koblas.core.F64DenseVector
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class Exp4BanditAllocationTest {

    private val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean

    private fun interface Body {
        fun run()
    }

    private fun bytesPerCall(body: Body): Double {
        repeat(1_000) { body.run() }
        val id = Thread.currentThread().threadId()
        var best = Double.MAX_VALUE
        repeat(5) {
            val before = bean.getThreadAllocatedBytes(id)
            repeat(2_000) { body.run() }
            val after = bean.getThreadAllocatedBytes(id)
            best = minOf(best, (after - before).toDouble() / 2_000)
        }
        return best
    }

    @Test
    fun `destination distribution removes Kumulant output allocation with reusable advice`() {
        val allocating = reusableBandit()
        val destination = reusableBandit()
        val x = F64DenseVector.of(doubleArrayOf(1.0))
        val out = DoubleArray(ARMS)

        val allocatingBytes = bytesPerCall { allocating.playDistribution(x) }
        val destinationBytes = bytesPerCall { destination.playDistributionInto(x, out) }

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

        val chooseBytes = bytesPerCall { choosing.choose(x) }
        val updateBytes = bytesPerCall {
            updating.update(arm, x, reward = 0.0)
            updating.choose(x)
        } / 2.0

        assertTrue(chooseBytes < 128.0, "choose allocated $chooseBytes B/call")
        assertTrue(updateBytes < 128.0, "update allocated $updateBytes B/call")
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
