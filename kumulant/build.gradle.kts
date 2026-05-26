@file:OptIn(ExperimentalWasmDsl::class)

import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile

plugins {
    id("com.eignex.kmp") version "1.2.1"
    kotlin("plugin.serialization") version "2.3.20"
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
    js(IR) { browser(); nodejs() }
    wasmJs { browser(); nodejs() }
    wasmWasi { nodejs() }
    linuxX64(); linuxArm64()
    macosX64(); macosArm64(); mingwX64()
    iosX64(); iosArm64(); iosSimulatorArm64()

    sourceSets {
        val nonJvmMain by creating {
            dependsOn(commonMain.get())
        }
        nativeMain.get().dependsOn(nonJvmMain)
        jsMain.get().dependsOn(nonJvmMain)
        wasmJsMain.get().dependsOn(nonJvmMain)
        wasmWasiMain.get().dependsOn(nonJvmMain)

        val posixMain by creating { dependsOn(nativeMain.get()) }
        appleMain.get().dependsOn(posixMain)
        linuxMain.get().dependsOn(posixMain)
        webMain.get().dependsOn(nonJvmMain)
        wasmWasiMain.get().dependsOn(webMain.get())
        commonMain.dependencies {
            api("com.eignex:skema:0.1.1")
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-core:1.10.0")
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
        }
        jvmMain.dependencies {
            implementation(kotlin("reflect"))
        }
        commonTest.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.10.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.10.0")
        }
    }
}

// Dokka site is the canonical user documentation. Module-level and per-package prose
// lives in adjacent .md files; runnable code examples live as samples under
// src/commonTest and are referenced from KDoc with `@sample`.
dokka {
    moduleName.set("kumulant")
    dokkaSourceSets.named("commonMain") {
        includes.from(
            "module.md",
            "src/commonMain/kotlin/com/eignex/kumulant/stat/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/operation/package.md",
            "src/commonMain/kotlin/com/eignex/kumulant/schema/package.md",
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
