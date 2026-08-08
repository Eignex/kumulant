@file:OptIn(ExperimentalWasmDsl::class)

import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile

plugins {
    id("com.eignex.kmp") version "1.2.8"
    kotlin("plugin.serialization") version "2.4.10"
}

eignexPublish {
    description.set("Streaming statistics, sketches, and online bandits for Kotlin/KMP.")
    githubRepo.set("Eignex/kumulant")
}

kotlin {
    applyDefaultHierarchyTemplate()
    compilerOptions {
        freeCompilerArgs.add("-Xexpect-actual-classes")
    }
    jvm()
    js { browser(); nodejs() }
    wasmJs { browser(); nodejs() }
    wasmWasi { nodejs() }
    linuxX64(); linuxArm64()
    macosArm64(); mingwX64()
    iosX64(); iosArm64(); iosSimulatorArm64()

    sourceSets {
        val nonJvmMain = create("nonJvmMain") {
            dependsOn(commonMain.get())
        }
        nativeMain.get().dependsOn(nonJvmMain)
        wasmWasiMain.get().dependsOn(nonJvmMain)

        val posixMain = create("posixMain") { dependsOn(nativeMain.get()) }
        appleMain.get().dependsOn(posixMain)
        linuxMain.get().dependsOn(posixMain)
        webMain.get().dependsOn(nonJvmMain)
        wasmWasiMain.get().dependsOn(webMain.get())
        commonMain.dependencies {
            api("com.eignex:koblas:0.1.0")
            api("com.eignex:skema:0.3.0")
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-core:1.11.0")
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
        }
        jvmMain.dependencies {
            implementation(kotlin("reflect"))
        }
        commonTest.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.11.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.11.0")
        }
    }
}

// Dokka site is the canonical user documentation. Module-level and per-package prose
// lives in adjacent .md files; runnable code examples live as samples under
// src/commonTest and are referenced from KDoc with `@sample`.
dokka {
    moduleName.set("kumulant")
    dokkaSourceSets.configureEach {
        sourceLink {
            localDirectory.set(projectDir.resolve("src"))
            val sub = projectDir.relativeTo(rootDir).invariantSeparatorsPath
            val prefix = if (sub.isEmpty()) "src" else "$sub/src"
            remoteUrl("https://github.com/Eignex/${rootProject.name}/blob/main/$prefix")
            remoteLineSuffix.set("#L")
        }
    }
    dokkaSourceSets.named("commonMain") {
        includes.from(
            "module.md",
            "src/commonMain/kotlin/com/eignex/kumulant/core/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/summary/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/regression/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/quantile/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/cardinality/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/sketch/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/event/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/rate/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/change/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/decay/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/forecast/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/score/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/anomaly/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/calibration/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/regression/glm/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/regression/tree/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/math/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/bandit/univariate/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/bandit/contextual/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/schema/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/schema/spec/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/schema/expr/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/schema/runtime/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/schema/ops/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/schema/optimizer/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/schema/decay/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/bandit/package.md",
        )
        samples.from("src/commonMain/kotlin/com/eignex/kumulant/samples")
    }
}

// JVM SIMD primitives in com.eignex.kumulant.math.Primitives.kt use the incubator
// Vector API. Make the module visible to the Kotlin compiler and at test runtime;
// downstream JVM consumers need the same flag.
tasks.withType<KotlinJvmCompile>().configureEach {
    compilerOptions.freeCompilerArgs.add("-Xadd-modules=jdk.incubator.vector")
}
tasks.withType<Test>().configureEach {
    if (project.findProperty("kumulant.noSimd") != "true") {
        jvmArgs("--add-modules=jdk.incubator.vector")
    }
}
