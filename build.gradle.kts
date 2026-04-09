@file:OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)

plugins {
    id("com.eignex.kmp") version "1.1.4"
    kotlin("plugin.serialization") version "2.3.0"
}

eignexPublish {
    description.set("Pure Kotlin multiplatform streaming statistics library.")
    githubRepo.set("Eignex/kumulant")
}

val kumulantGenerator by configurations.creating

kotlin {
    jvm()
    js(IR) { browser(); nodejs() }
    wasmJs { browser(); nodejs() }
    wasmWasi { nodejs() }
    linuxX64(); linuxArm64()
    macosX64(); macosArm64(); mingwX64()
    iosX64(); iosArm64(); iosSimulatorArm64()

    sourceSets {
        commonMain.dependencies {
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-core:1.10.0")
        }
        jvmMain.dependencies {
            implementation(kotlin("reflect"))
        }
        commonTest.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.10.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.10.0")
        }

        commonMain {
            kotlin.srcDir(files(layout.buildDirectory.dir("generated/source/kumulant")).builtBy("generateExtensions"))
        }
    }
}

dependencies {
    kumulantGenerator("org.jetbrains.kotlin:kotlin-compiler-embeddable:2.3.0")
}

val generatedSourceDir = layout.buildDirectory.dir("generated/source/kumulant")

val generateExtensions by tasks.registering(ExtensionGeneratorTask::class) {
    inputDir.set(file("src/commonMain/kotlin"))
    outputFile.set(generatedSourceDir.map { it.file("com/eignex/kumulant/core/Extensions.kt") })
    compilerClasspath.from(kumulantGenerator)
}
