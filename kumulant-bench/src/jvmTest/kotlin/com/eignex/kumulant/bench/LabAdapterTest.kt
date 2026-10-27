package com.eignex.kumulant.bench

import com.eignex.lab.bench.BenchRequest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class LabAdapterTest {
    @Test fun `JMH argv is frozen without losing repeated arguments and sizes the CPU reservation`() {
        val argv = "[\"-t\",\"4\",\"-p\",\"name=SumStat\",\"-p\",\"concurrency=Strict\"]"
        val case = KumulantLabSuite.plan(BenchRequest(mapOf("backends" to "scalar", "argv" to argv))).single()
        assertEquals(argv, case.parameters["argv"])
        assertEquals(4, case.resources.cpus)
    }

    @Test fun `prepared native plans pin both executables and the Linux platform`() {
        val case = KumulantLabSuite.plan(BenchRequest(mapOf("backends" to "native"))).single()
        assertEquals(2, case.inputs.size)
        assertEquals(setOf("platform:linux", "arch:x86_64"), case.resources.requiredTags)
    }

    @Test fun `Native concurrency requests are refused before execution`() {
        assertFailsWith<IllegalArgumentException> { KumulantLabSuite.plan(BenchRequest(mapOf("backends" to "native", "mode" to "concurrency"))) }
    }
}
