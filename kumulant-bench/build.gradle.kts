import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile

plugins {
    kotlin("multiplatform") version "2.3.0"
    kotlin("plugin.allopen") version "2.3.0"
    id("org.jetbrains.kotlinx.benchmark") version "0.4.13"
}

repositories {
    mavenCentral()
}

kotlin {
    applyDefaultHierarchyTemplate()
    compilerOptions {
        freeCompilerArgs.add("-Xexpect-actual-classes")
    }

    jvm()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":kumulant"))
            implementation("org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.13")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.10.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
        }
        jvmTest.dependencies {
            implementation(kotlin("test-junit"))
        }
    }
}

allOpen {
    annotation("org.openjdk.jmh.annotations.State")
}

benchmark {
    targets {
        register("jvm")
        register("linuxX64")
    }
    configurations {
        named("main") {
            warmups = 3
            iterations = 5
            iterationTime = 1
            iterationTimeUnit = "s"
        }
    }
}

tasks.withType<KotlinJvmCompile>().configureEach {
    compilerOptions.freeCompilerArgs.add("-Xadd-modules=jdk.incubator.vector")
}
tasks.withType<Test>().configureEach {
    jvmArgs("--add-modules=jdk.incubator.vector")
}

// Analysis tasks. Each one drives the StatSpec registry and prints a measurement
// table to stdout — these are not pass/fail tests, they are reports you read.
fun JavaExec.kumulantBenchSetup() {
    group = "bench"
    classpath = kotlin.jvm().compilations.getByName("main").let {
        it.output.allOutputs + it.runtimeDependencyFiles
    }
    jvmArgs("--add-modules=jdk.incubator.vector")
    // Forward `-Dbench.*` from the gradle invocation onto the forked JVM so users
    // can tune cell duration, thread count, and JFR options without editing code.
    System.getProperties().forEach { k, v ->
        val key = k.toString()
        if (key.startsWith("bench.")) systemProperty(key, v.toString())
    }
}

tasks.register<JavaExec>("analyzeAccuracy") {
    description = "Per-stat serial accuracy vs analytical reference. Prints to stdout."
    mainClass.set("com.eignex.kumulant.bench.AccuracyAnalysisKt")
    kumulantBenchSetup()
}

tasks.register<JavaExec>("analyzeConcurrencyDrift") {
    description = "Per-stat update-path drift under each Concurrency level, 4 threads. Prints to stdout."
    mainClass.set("com.eignex.kumulant.bench.ConcurrencyDriftAnalysisKt")
    kumulantBenchSetup()
}

tasks.register<JavaExec>("analyzeThroughput") {
    description = "Per-stat update throughput at 1 and N threads × each Concurrency level. Optional JFR via -Dbench.jfr=true. Prints to stdout."
    mainClass.set("com.eignex.kumulant.bench.ThroughputAnalysisKt")
    kumulantBenchSetup()
}

tasks.register<JavaExec>("analyzeBanditThroughput") {
    description = "Per-bandit (choose, play, update) cycles/sec at 1 and N threads. Prints to stdout."
    mainClass.set("com.eignex.kumulant.bench.BanditThroughputAnalysisKt")
    kumulantBenchSetup()
}

tasks.register<JavaExec>("analyzeBanditAccuracy") {
    description = "Per-bandit cumulative regret under a known reward model. Prints to stdout."
    mainClass.set("com.eignex.kumulant.bench.BanditAccuracyAnalysisKt")
    kumulantBenchSetup()
}

tasks.register<JavaExec>("analyzeBanditDrift") {
    description = "Per-bandit concurrent-update drift over N threads sharing the same bandit. Prints to stdout."
    mainClass.set("com.eignex.kumulant.bench.BanditDriftAnalysisKt")
    kumulantBenchSetup()
}

tasks.register<JavaExec>("analyzeMergeContention") {
    description = "Per-stat merge-path drift when 4 threads concurrently merge snapshots. Prints to stdout."
    mainClass.set("com.eignex.kumulant.bench.MergeContentionAnalysisKt")
    kumulantBenchSetup()
}
