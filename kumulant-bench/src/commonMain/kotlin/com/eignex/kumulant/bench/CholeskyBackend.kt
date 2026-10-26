package com.eignex.kumulant.bench

import com.eignex.koblas.koblas

internal fun reportCholeskyBackend() {
    println("Cholesky backend: $koblas")
    val vendor = koblas.vendor
    if (vendor == null) {
        println("Cholesky vendor: none")
    } else {
        println("Cholesky vendor: ${vendor.vendor}")
        println("Cholesky library: ${vendor.libraryPath}")
        println("Cholesky version: ${vendor.version}")
        println("Cholesky threads: ${vendor.threadEvidence.label}")
    }
}
