import org.gradle.jvm.tasks.Jar
import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile
import org.jetbrains.kotlin.gradle.dsl.KotlinMultiplatformExtension

plugins {
    kotlin("multiplatform") version "2.4.21"
    kotlin("plugin.allopen") version "2.4.21"
    id("org.jetbrains.kotlinx.benchmark") version "0.4.17"
}

repositories {
    mavenCentral()
    // The pinned koblas snapshot arrives transitively through :kumulant.
    maven("https://central.sonatype.com/repository/maven-snapshots/") {
        mavenContent { snapshotsOnly() }
    }
}

// :kumulant gets its toolchain from the com.eignex.kmp convention plugin, which this module does not
// apply (it publishes nothing). Keep the benchmark runtime on the library's JDK.
val benchJdk = 25

kotlin {
    applyDefaultHierarchyTemplate()
    compilerOptions {
        freeCompilerArgs.add("-Xexpect-actual-classes")
    }
    jvmToolchain(benchJdk)

    jvm()
    linuxX64 {
        binaries.executable("choleskyQuality") {
            entryPoint = "com.eignex.kumulant.bench.main"
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(project(":kumulant"))
            implementation("org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.17")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.12.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
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
        // kotlinx-benchmark resolves the Kotlin/Native host while wiring Native tasks, which fails on
        // Linux arm64; JVM benchmarks still run there.
        val linuxArm64Host = System.getProperty("os.name") == "Linux" && System.getProperty("os.arch") == "aarch64"
        if (!linuxArm64Host) register("linuxX64")
    }
    reportsDir = providers.gradleProperty("bench.output").getOrElse("build/reports/benchmarks")
    configurations {
        named("main") {
            val smoke = providers.gradleProperty("bench.smoke").getOrElse("false").toBoolean()
            warmups = if (smoke) 1 else 3
            iterations = if (smoke) 2 else 5
            iterationTime = if (smoke) 100 else 1000
            iterationTimeUnit = "ms"
            reportFormat = "json"
            include(providers.gradleProperty("bench.filter").getOrElse(".*(UpdateThroughputBenchmark.update|BanditBenchmark.cycle)"))
            param("name", *providers.gradleProperty("bench.stats").getOrElse("SumStat").split(',').toTypedArray())
            param("policy", *providers.gradleProperty("bench.policies").getOrElse("MultiArmed/Thompson").split(',').toTypedArray())
            param("concurrency", "None")
            providers.gradleProperty("bench.mode").orNull?.let { mode = it }
            providers.gradleProperty("bench.timeUnit").orNull?.let { outputTimeUnit = it }
            for ((key, value) in providers.gradlePropertiesPrefixedBy("bench.param.").get()) {
                param(key.removePrefix("bench.param."), *value.split(',').toTypedArray())
            }
        }
    }
}

tasks.withType<KotlinJvmCompile>().configureEach {
    compilerOptions.freeCompilerArgs.add("-Xadd-modules=jdk.incubator.vector")
}
tasks.withType<Test>().configureEach {
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    if (project.findProperty("kumulant.noSimd") != "true") {
        jvmArgs("--add-modules=jdk.incubator.vector")
    }
}

abstract class PrepareBenchmark : DefaultTask() {
    @get:Input abstract val javaExecutable: Property<String>
    @get:OutputFile abstract val javaFile: RegularFileProperty

    @TaskAction
    fun writeJava() {
        javaFile.get().asFile.apply { parentFile.mkdirs(); writeText(javaExecutable.get() + "\n") }
    }
}

tasks.register<PrepareBenchmark>("prepareBenchmark") {
    group = "bench"
    description = "Build the JMH harness and record its toolchain Java executable."
    dependsOn("jvmBenchmarkJar")
    javaExecutable.set(javaToolchains.launcherFor {
        languageVersion.set(JavaLanguageVersion.of(benchJdk))
    }.map { it.executablePath.asFile.absolutePath })
    javaFile.set(layout.buildDirectory.file("benchmark-java"))
}

tasks.withType<Jar>().matching { it.name == "jvmBenchmarkJar" }.configureEach {
    manifest.attributes["Multi-Release"] = "true"
    dependsOn("jvmJar", ":kumulant:jvmJar")
    // Project outputs must be inputs even when their JARs do not exist during configuration.
    from(kotlin.jvm().compilations.getByName("main").output.allOutputs)
    from(project(":kumulant").extensions.getByType<KotlinMultiplatformExtension>()
        .jvm().compilations.getByName("main").output.allOutputs)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
