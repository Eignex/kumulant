package com.eignex.kumulant.bandit.contextual

import com.eignex.koblas.DenseVector
import com.eignex.koblas.Workspace
import com.eignex.kumulant.assertAllocatesAtMost
import kotlin.test.Test

class KnnWorkspaceAllocationTest {

    private fun populatedBandit(): KnnContextualBandit = KnnContextualBandit(
        ARMS,
        K,
        exploration = 0.0,
    ).also { bandit ->
        repeat(ARMS) { arm ->
            repeat(HISTORY) { sample ->
                bandit.update(
                    arm,
                    DenseVector.of(DoubleArray(FEATURES) { (it + sample).toDouble() }),
                    sample.toDouble(),
                )
            }
        }
    }

    @Test
    fun `k nearest neighbour scoring allocates nothing with or without a workspace`() {
        val bare = populatedBandit()
        val reused = populatedBandit()
        val x = DenseVector.of(DoubleArray(FEATURES) { it * 0.25 })
        val workspace = Workspace()

        // The scan buffer is owned by the bandit, so there is nothing left for a workspace to save
        // and nothing left to allocate when one is absent. A buffer coming back would be a
        // `DoubleArray(3 * K)`, about 208 B, spent once per arm scored.
        assertAllocatesAtMost(0.0, "choose without a workspace") { bare.choose(x) }
        assertAllocatesAtMost(0.0, "choose with a workspace") { reused.choose(x, workspace) }
    }

    private companion object {
        const val ARMS = 4
        const val K = 8
        const val HISTORY = 32
        const val FEATURES = 32
    }
}
