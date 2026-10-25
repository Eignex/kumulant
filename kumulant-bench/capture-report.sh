#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
    cat <<'HELP'
Usage: kumulant-bench/capture-report.sh [options]
  --preset smoke|standard|extended   Case selection and timing budget (default: standard)
  --suite stats|cholesky             Select one suite (default: both)
  --case PATTERN                     Match stable case IDs; quote * wildcards
  --mode quality|performance         Default: performance; always validates quality first
  --targets jvm-scalar,jvm-simd      Comma-separated backends (default: both)
  --allocation                      Enable the JMH GC allocation profiler
  --output DIR                      New or empty directory (default: build/captures/<run>)
  --list                            Print selected case definitions without measuring

Requires the repository's Gradle/JDK toolchain. Fixtures and presets live in Kotlin.
Smoke captures check wiring; their timings are not performance baselines.
HELP
    exit 0
fi

./gradlew --console=plain :kumulant-bench:prepareCapture
IFS= read -r CAPTURE_JAVA < kumulant-bench/build/capture/java
CAPTURE_JAR="$ROOT/kumulant-bench/build/benchmarks/jvm/jars/kumulant-bench-jvm-jmh-JMH.jar"
exec "$CAPTURE_JAVA" -cp "$CAPTURE_JAR" com.eignex.kumulant.bench.CaptureMainKt "$@"
