#!/usr/bin/env bash
set -euo pipefail
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

smoke=false
native=false
scalar=false
mode=performance
output=""
while (($#)); do
    case "$1" in
        --output) output=${2:?--output requires a directory}; shift 2 ;;
        --smoke) smoke=true; shift ;;
        --native) native=true; shift ;;
        --scalar) scalar=true; shift ;;
        --quality) mode=quality; shift ;;
        --concurrency) mode=concurrency; shift ;;
        --help) echo 'Usage: benchmark.sh [--output DIR] [--smoke] [--scalar|--native] [--quality|--concurrency] [-- JMH/Gradle arguments]'; exit 0 ;;
        --) shift; break ;;
        *) break ;;
    esac
done
if [[ "$native" == true ]]; then
    [[ "$scalar" == false && "$mode" != concurrency && "$(uname -s)/$(uname -m)" == Linux/x86_64 ]] || { echo 'Native requires Linux x86_64 and uses its default backend.' >&2; exit 2; }
fi
if [[ -z "$output" ]]; then
    mkdir -p kumulant-bench/build/runs
    output=$(mktemp -d "$PWD/kumulant-bench/build/runs/run.XXXXXX")
else
    mkdir -p "$output"
    output=$(cd "$output" && pwd)
    shopt -s nullglob dotglob
    entries=("$output"/*)
    ((${#entries[@]} == 0)) || { echo "Output is not empty: $output" >&2; exit 2; }
fi
echo "Reports: $output"
./gradlew --console=plain :kumulant-bench:prepareBenchmark
IFS= read -r java < kumulant-bench/build/benchmark-java
jvm_args=(--enable-native-access=ALL-UNNAMED -Xms512m -Xmx512m "-Dbench.smoke=$smoke")
[[ "$scalar" == true ]] || jvm_args+=(--add-modules=jdk.incubator.vector)
runner=("$java" "${jvm_args[@]}" -cp "$PWD/kumulant-bench/build/benchmarks/jvm/jars/kumulant-bench-jvm-jmh-JMH.jar" com.eignex.kumulant.bench.BenchmarkMainKt)
{
    git rev-parse HEAD
    git status --short
    uname -a
    "$java" -version
    printf 'smoke=%s native=%s scalar=%s mode=%s\narguments: ' "$smoke" "$native" "$scalar" "$mode"
    printf '%q ' "$@"
    echo
} > "$output/run.txt" 2>&1
git diff --binary HEAD > "$output/source.patch"
"${runner[@]}" --quality > "$output/quality.txt" 2>&1
if [[ "$native" == true ]]; then
    parameters=$("${runner[@]}" --parameters)
    stats=${parameters%%$'\n'*}
    policies=${parameters#*$'\n'}
    if [[ "$smoke" == true ]]; then stats=SumStat; policies=MultiArmed/Thompson; fi
    tasks=(:kumulant-bench:linuxX64Test)
    [[ "$mode" == quality ]] || tasks+=(:kumulant-bench:linuxX64Benchmark)
    ./gradlew --console=plain "${tasks[@]}" "-Pbench.output=$output/native" "-Pbench.smoke=$smoke" "-Pbench.stats=$stats" "-Pbench.policies=$policies" "$@" 2>&1 | tee "$output/benchmark.txt"
elif [[ "$mode" == concurrency ]]; then
    "${runner[@]}" --concurrency > "$output/concurrency.txt" 2>&1
elif [[ "$mode" == performance ]]; then
    "${runner[@]}" "$@" -rf json -rff "$output/results.json" 2>&1 | tee "$output/benchmark.txt"
fi
