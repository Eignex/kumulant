package com.eignex.kumulant.bench

import com.eignex.lab.bench.MetricDirection
import java.nio.file.Files
import java.nio.file.Path
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LabMeasurementsTest {
    private fun reports(action: (Path) -> Unit) {
        val root = Files.createTempDirectory("kumulant-reports")
        try { action(root) } finally { root.toFile().deleteRecursively() }
    }

    @Test fun `nested JMH reports preserve secondary metrics units directions and provenance`() = reports { root ->
        val report = Files.createDirectories(root.resolve("pass-2/nested"))
        Files.writeString(report.resolve("results.json"), """[
            {"jmhVersion":"1.37","benchmark":"com.eignex.kumulant.bench.UpdateThroughputBenchmark.update","mode":"thrpt","params":{"name":"SumStat"},
             "primaryMetric":{"score":12.0,"scoreError":"NaN","scoreUnit":"ops/s","rawData":[[11.0,13.0]]},
             "secondaryMetrics":{"gc.alloc.rate.norm":{"score":8.0,"scoreUnit":"B/op"},"gc.count":{"score":"NaN","scoreUnit":"counts"}}}
        ]""")
        val result = labMeasurements(root)
        assertTrue(result.errors.isEmpty())
        assertEquals(1, result.primaryCount)
        val primary = result.metrics.single { it.name.endsWith("/primary") }
        assertEquals(12.0, primary.value)
        assertEquals("ops/s", primary.unit)
        assertEquals(MetricDirection.HIGHER, primary.direction)
        assertEquals(MetricDirection.LOWER, result.metrics.single { it.unit == "B/op" }.direction)
        val source = result.details.getValue("metricSources").jsonObject.getValue(primary.name).jsonObject
        assertEquals("pass-2/nested/results.json", source.getValue("report").jsonPrimitive.content)
        assertEquals("4096", source.getValue("operationsPerInvocation").jsonPrimitive.content)
        assertEquals("SumStat", source.getValue("params").jsonObject.getValue("name").jsonPrimitive.content)
    }

    @Test fun `malformed later reports retain earlier native measurements`() = reports { root ->
        val first = Files.createDirectories(root.resolve("native-0"))
        val second = Files.createDirectories(root.resolve("native-1"))
        Files.writeString(first.resolve("results.json"), """[{"benchmark":"factor","mode":"avgt","primaryMetric":{"score":2.5,"scoreUnit":"us/op","rawData":[[2.0,3.0]]}}]""")
        Files.writeString(second.resolve("results.json"), "[")
        val result = labMeasurements(root)
        assertEquals(2.5, result.metrics.single().value)
        assertEquals(MetricDirection.LOWER, result.metrics.single().direction)
        assertEquals(1, result.errors.size)
    }

    @Test fun `quality concurrency and stress metrics retain their row identities`() = reports { root ->
        Files.writeString(root.resolve("quality.txt"), """
            normal_sampling,samples,dimensions,rmse; seeds=0..7
            sobol,256,4,0.01
            stat,actual,reference,scaled_error
            SumStat,3,3,0
            bandit,mean_pseudo_regret,standard_error; seeds=0..4
            MultiArmed/Thompson,12,1
            fixture,size,scale,relative_reconstruction,solve_backward_error
            precision,63,1.0,1e-16,2e-17
            diagonal,size,scale,absolute_log_determinant_error
            diagonal,65,1.0,0
            update,size,scale,reconstruction,backward_error,relative_solution_error,absolute_logdet_error
            update,8,1.0,0,1e-17,0,0
            extension,kernel,size,appended,scale,reconstruction,backward_error,relative_solution_error,absolute_logdet_error,forward_tolerance
            extension,rbf,63,4,1.0,0,0,0,0,1e-10
            retry,scenario,attempts,diagonal_shift,succeeded
            retry,exhaustion,3,1e-8,false
        """.trimIndent())
        Files.writeString(root.resolve("concurrency.txt"), "stat,level,operation,threads,count_error,scaled_drift,snapshot_reads\nMeanStat,Relaxed,merge,4,0,0.02,50\n")
        Files.writeString(root.resolve("native-stress.txt"), "stress_factor,kernel,order,relative_reconstruction,solve_backward_error\nstress_factor,rbf,32,1e-16,2e-17\n")
        val result = labMeasurements(root)
        assertTrue(result.errors.isEmpty())
        assertEquals(26, result.metrics.size)
        assertEquals(0.02, result.metrics.single { it.name.endsWith("/scaled_drift") }.value)
        assertEquals("reads", result.metrics.single { it.name.endsWith("/snapshot_reads") }.unit)
        assertTrue(result.metrics.filter { it.name.endsWith("error") && !it.name.endsWith("standard_error") }.all { it.direction == MetricDirection.LOWER })
        assertEquals(0.0, result.metrics.single { it.name.endsWith("/succeeded") }.value)
    }
}
