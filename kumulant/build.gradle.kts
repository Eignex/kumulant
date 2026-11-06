plugins {
    id("com.eignex.kmp")
    id("com.eignex.publish.github")
    kotlin("plugin.serialization")
}

eignexPublish {
    description.set("Streaming statistics, sketches, and online bandits for Kotlin/KMP.")
    githubRepo.set("Eignex/kumulant")
}

repositories {
    ivy {
        url = uri("https://github.com/Eignex/kbuild/releases/download")
        patternLayout { artifact("v[revision]/[artifact]-[revision](-[classifier]).[ext]") }
        metadataSources { gradleMetadata() }
        content { includeModule("com.eignex", "kbuild-platform") }
    }
    maven("https://central.sonatype.com/repository/maven-snapshots/") {
        mavenContent { snapshotsOnly() }
    }
}

val utilPin = "0.0.1-20261105.215902-10"

// Routine checks cover JVM + Linux x64; release validation opts into every supported target.
val fullTargets = providers.gradleProperty("targets.full").isPresent

eignexBuild {
    jvmToolchain.set(17)
    abiValidationEnabled.set(fullTargets)
}

kotlin {
    jvm()
    linuxX64()
    if (fullTargets) {
        linuxArm64()
        macosArm64(); mingwX64()
        iosArm64(); iosSimulatorArm64()
    }

    sourceSets {
        commonMain.dependencies {
            implementation("com.eignex:util:$utilPin") {
                version { strictly(utilPin) }
            }
            api("com.eignex:koblas:0.1.1-20261105.140327-312") {
                version { strictly("0.1.1-20261105.140327-312") }
            }
            api("com.eignex:skema:0.4.0")
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-core")
            compileOnly("org.jetbrains.kotlinx:kotlinx-serialization-json")
        }
        commonTest.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-core")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-json")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-protobuf")
            implementation("org.jetbrains.kotlinx:kotlinx-serialization-cbor")
        }
    }
}

// Dependency, generated serializer and test harness bytecode is outside this module's coverage report.
kover {
    currentProject {
        instrumentation {
            includedClasses.add("com.eignex.kumulant.*")
            excludedClasses.addAll("com.eignex.kumulant.*Test", "com.eignex.kumulant.*Test\$*")
            excludedClasses.add("com.eignex.kumulant.*\$\$serializer")
        }
    }
}

// Timestamped KMP roots redirect to mutable platform snapshots. Publish constraints so downstream
// Gradle consumers resolve the same binaries as this build.
dependencies {
    constraints {
        for ((module, pinned) in mapOf(
            "jvm" to utilPin,
            "linuxx64" to utilPin,
            "linuxarm64" to utilPin,
            "macosarm64" to utilPin,
            "mingwx64" to utilPin,
            "iosarm64" to "0.0.1-20261105.215902-11",
            "iossimulatorarm64" to "0.0.1-20261105.215902-11",
        )) {
            add("commonMainImplementation", "com.eignex:util-$module:$pinned") {
                version { strictly(pinned) }
            }
        }
        for ((library, platforms) in mapOf(
            "koblas" to mapOf(
                "jvm" to "0.1.1-20261105.140327-315",
                "linuxx64" to "0.1.1-20261105.140327-310",
                "linuxarm64" to "0.1.1-20261105.140327-312",
                "macosarm64" to "0.1.1-20261105.140327-334",
                "mingwx64" to "0.1.1-20261105.140327-138",
                "iosarm64" to "0.1.1-20261105.140327-159",
                "iossimulatorarm64" to "0.1.1-20261105.140327-159",
            ),
        )) {
            for ((module, pinned) in platforms) {
                add("commonMainApi", "com.eignex:$library-$module:$pinned") {
                    version { strictly(pinned) }
                }
            }
        }
    }
}

// Dokka site is the canonical user documentation. Module-level and per-package prose
// lives in adjacent .md files; compile-checked examples live under
// src/commonMain/kotlin/com/eignex/kumulant/samples and are referenced with `@sample`.
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
            "src/commonMain/kotlin/com/eignex/kumulant/bandit/package.md",
        )
        samples.from("src/commonMain/kotlin/com/eignex/kumulant/samples")
    }
}

// Select an installed simulator in CI rather than relying on a particular iPhone model.
tasks.withType<org.jetbrains.kotlin.gradle.targets.native.tasks.KotlinNativeSimulatorTest>().configureEach {
    providers.gradleProperty("iosSimulatorDevice").orNull?.let { device.set(it) }
}
