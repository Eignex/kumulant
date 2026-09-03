package com.eignex.kumulant.bandit.contextual

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseVector
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import kotlin.test.Test
import kotlin.test.assertTrue

class KnnWorkspaceAllocationTest {

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

    private fun populatedBandit(): KnnContextualBandit = KnnContextualBandit(
        ARMS,
        K,
        exploration = 0.0,
    ).also { bandit ->
        repeat(ARMS) { arm ->
            repeat(HISTORY) { sample ->
                bandit.update(
                    arm,
                    F64DenseVector.of(DoubleArray(FEATURES) { (it + sample).toDouble() }),
                    sample.toDouble(),
                )
            }
        }
    }

    @Test
    fun `k nearest neighbour scoring allocates nothing with or without a workspace`() {
        val bare = populatedBandit()
        val reused = populatedBandit()
        val x = F64DenseVector.of(DoubleArray(FEATURES) { it * 0.25 })
        val workspace = Workspace().apply { reserve(3 * K, 1) }

        val bareBytes = bytesPerCall { bare.choose(x) }
        val workspaceBytes = bytesPerCall { reused.choose(x, workspace) }

        // The scan buffer is owned by the bandit, so there is nothing left for a workspace to save
        // and nothing left to allocate when one is absent.
        assertTrue(bareBytes <= CEILING, "choose allocated $bareBytes B/call without a workspace")
        assertTrue(workspaceBytes <= CEILING, "choose allocated $workspaceBytes B/call with a workspace")
    }

    private companion object {
        const val ARMS = 4
        const val K = 8
        const val HISTORY = 32
        const val FEATURES = 32

        /** Slack for sampling noise in the allocation counter; a real per-call buffer is 3 * K * 8 B. */
        const val CEILING = 8.0
    }
}
