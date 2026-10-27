include(":kumulant", ":kumulant-bench")

// Measurement only, never merged: substitute Koblas built from one pinned source commit for the published snapshot,
// so matched arms differ only in the commit named here and in Kumulant's own sources.
val koblasCommit = "5eb646d147632e1c6f003001db4ddb53afce995a"
val koblasSource = file("build/koblas-source")
fun git(vararg args: String): String =
    providers.exec { commandLine("git", *args) }.standardOutput.asText.get().trim()
if (!koblasSource.resolve(".git").isDirectory) {
    git("clone", "-q", "https://github.com/Eignex/koblas.git", koblasSource.path)
}
if (git("-C", koblasSource.path, "rev-parse", "HEAD") != koblasCommit) {
    git("-C", koblasSource.path, "fetch", "-q", "origin", koblasCommit)
    git("-C", koblasSource.path, "checkout", "-q", "--detach", koblasCommit)
}
check(git("-C", koblasSource.path, "rev-parse", "HEAD") == koblasCommit) { "Koblas source is not $koblasCommit" }
check(git("-C", koblasSource.path, "status", "--porcelain").isEmpty()) { "Koblas source has local changes" }
println("Koblas source substituted from $koblasCommit")
includeBuild(koblasSource) {
    dependencySubstitution { substitute(module("com.eignex:koblas")).using(project(":koblas")) }
}
