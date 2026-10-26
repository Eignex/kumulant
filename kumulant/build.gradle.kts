plugins {
    id("com.eignex.kmp") version "1.3.5"
    kotlin("plugin.serialization") version "2.4.21"
}

eignexPublish {
    description.set("Streaming statistics, sketches, and online bandits for Kotlin/KMP.")
    githubRepo.set("Eignex/kumulant")
}

repositories {
    maven("https://central.sonatype.com/repository/maven-snapshots/") {
        mavenContent { snapshotsOnly() }
    }
}

kotlin {
    applyDefaultHierarchyTemplate()
    compilerOptions {
        freeCompilerArgs.add("-Xexpect-actual-classes")
    }
    jvmToolchain(25)
    jvm {
        compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_25)
    }
    linuxX64(); linuxArm64(); macosArm64()

    sourceSets {
        val nonJvmMain = create("nonJvmMain") {
            dependsOn(commonMain.get())
        }
        nativeMain.get().dependsOn(nonJvmMain)

        val posixMain = create("posixMain") { dependsOn(nativeMain.get()) }
        appleMain.get().dependsOn(posixMain)
        linuxMain.get().dependsOn(posixMain)
        commonMain.dependencies {
            api("com.eignex:koblas:0.1.1-20261025.142922-269") {
                version { strictly("0.1.1-20261025.142922-269") }
            }
            api("com.eignex:skema:0.3.0")
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-core:1.12.0")
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
        }
        jvmMain.dependencies {
            implementation(kotlin("reflect"))
        }
        commonTest.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.12.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:1.12.0")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-cbor:1.12.0")
        }
    }
}

// A timestamped KMP root redirects to mutable platform snapshots. Publish constraints so downstream
// Gradle consumers resolve the same binaries as this build.
dependencies {
    constraints {
        for ((module, pinned) in mapOf(
            "jvm" to "0.1.1-20261025.142922-272",
            "linuxx64" to "0.1.1-20261025.142922-268",
            "linuxarm64" to "0.1.1-20261025.142922-269",
            "macosarm64" to "0.1.1-20261025.142649-292",
        )) {
            add("commonMainApi", "com.eignex:koblas-$module:$pinned") {
                version { strictly(pinned) }
            }
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

// koblas's JVM SIMD kernels load only when the incubator Vector API module is open, so without this
// the tests cover its scalar fallback instead. `-Pkumulant.noSimd=true` drops it to reach that path.
tasks.withType<Test>().configureEach {
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    if (project.findProperty("kumulant.noSimd") != "true") {
        jvmArgs("--add-modules=jdk.incubator.vector")
    }
}
