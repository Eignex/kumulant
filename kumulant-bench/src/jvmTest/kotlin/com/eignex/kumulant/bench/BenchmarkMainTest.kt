package com.eignex.kumulant.bench

import org.openjdk.jmh.runner.options.CommandLineOptions
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BenchmarkMainTest {
    @Test
    fun `default stat parameters follow the shared registry`() {
        val options = benchmarkOptions(CommandLineOptions())

        assertEquals(allSpecs.map { it.name }, options.getParameter("name").get().toList())
    }

    @Test
    fun `explicit JMH parameters and timing are preserved`() {
        val options = benchmarkOptions(CommandLineOptions("-p", "name=VarianceStat", "-wi", "2", "-f", "3"))

        assertEquals(listOf("VarianceStat"), options.getParameter("name").get().toList())
        assertEquals(2, options.warmupIterations.get())
        assertEquals(3, options.forkCount.get())
    }

    @Test
    fun `concurrent defaults select validated synchronized stats`() {
        val options = benchmarkOptions(CommandLineOptions("-t", "4"))

        assertEquals(listOf("SumStat", "MeanStat", "VarianceStat"), options.getParameter("name").get().toList())
        assertEquals(listOf("Strict"), options.getParameter("concurrency").get().toList())
    }

    @Test
    fun `unsafe shared timing selections are rejected`() {
        for (args in listOf(
            arrayOf("-t", "4", "-p", "concurrency=None"),
            arrayOf("-t", "4", "-p", "name=RateStat"),
            arrayOf("-t", "4", "BanditBenchmark"),
        )) assertFailsWith<IllegalArgumentException> { benchmarkOptions(CommandLineOptions(*args)) }
    }
}
