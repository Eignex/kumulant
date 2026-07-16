rootProject.name = "kumulant"

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

include(":kumulant", ":kumulant-bench")
