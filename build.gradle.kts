plugins {
    // Declared here with apply=false so that two KMP subprojects don't each load
    // the Kotlin plugin into separate classloaders (which conflicts on shared
    // build services like KotlinNativeBundleBuildService).
    kotlin("multiplatform") version "2.3.20" apply false
    kotlin("plugin.allopen") version "2.3.20" apply false
    kotlin("plugin.serialization") version "2.3.20" apply false
    id("org.jetbrains.kotlinx.benchmark") version "0.4.13" apply false
}
