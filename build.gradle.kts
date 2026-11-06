plugins {
    // Declared here with apply=false so that two KMP subprojects don't each load
    // the Kotlin plugin into separate classloaders (which conflicts on shared
    // build services like KotlinNativeBundleBuildService).
    id("com.eignex.kmp") apply false
    id("com.eignex.publish.github") apply false
    kotlin("plugin.allopen") version "2.4.21" apply false
    kotlin("plugin.serialization") apply false
    id("org.jetbrains.kotlinx.benchmark") version "0.4.17" apply false
}
