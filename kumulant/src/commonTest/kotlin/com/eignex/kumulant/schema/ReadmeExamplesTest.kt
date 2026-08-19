package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// The README's composition and schema examples, compiled and executed: a snippet nothing compiles is
// indistinguishable from a correct one until someone tries it.
class ReadmeExamplesTest {

    private object Telemetry : StatSchema() {
        val latencyMean by series(Mean)
        val latencyP99 by series(DDSketch(probabilities = listOf(0.99)))
        val errorRate by series(Rate)
        val uniqueUsers by discrete(HyperLogLog(precision = 14))
    }

    @Test
    fun `composing on a spec then materializing works`() {
        val recentMean = Mean.windowed(durationMillis = 60_000, slices = 10).materialize()
        val positiveMean = Mean.filter(X gt 0.0).materialize()

        for (x in listOf(-5.0, 10.0, 20.0)) {
            recentMean.update(x)
            positiveMean.update(x)
        }

        // The filter drops the negative observation; the window keeps all three.
        assertEquals(15.0, positiveMean.read().mean, 1e-9)
        assertEquals(2.0, positiveMean.read().totalWeights, 1e-9)
        assertEquals(3.0, recentMean.read().totalWeights, 1e-9)
    }

    @Test
    fun `each modality in a mixed schema is driven by its own group`() {
        val latencies = StatGroup(Telemetry, concurrency = Concurrency.Strict)
        latencies.update(value = 12.7)
        latencies.update(value = 31.4)
        val p99 = latencies.read()[Telemetry.latencyP99]
        assertTrue(p99.quantiles.isNotEmpty(), "expected the sketch to report a p99")
        assertEquals(2.0, latencies.read()[Telemetry.latencyMean].totalWeights, 1e-9)

        val users = DiscreteStatGroup(Telemetry, concurrency = Concurrency.Strict)
        users.update(value = 0x9E3779B97F4A7C15uL.toLong())
        users.update(value = 0x243F6A8885A308D3uL.toLong())
        val distinct = users.read()[Telemetry.uniqueUsers]
        assertTrue(distinct.estimate > 0.0, "expected a non-zero cardinality estimate")
    }
}
