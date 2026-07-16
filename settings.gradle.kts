rootProject.name = "kumulant"

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

// Composite build: substitute com.eignex:koblas with the sibling koblas repo (linear algebra),
// so kumulant builds against local koblas without a published artifact.
includeBuild("../koblas")

include(":kumulant", ":kumulant-bench")
