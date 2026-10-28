#!/usr/bin/env bash
set -euo pipefail
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

prepared=false
smoke=false
native=false
scalar=false
mode=performance
output=""
repetitions=1
heap=512
gc=g1
stress=""
jfr=false
while (($#)); do
    case "$1" in
        --repetitions) repetitions=${2:?--repetitions requires a count}; shift 2 ;;
        --heap-mib) heap=${2:?--heap-mib requires a size}; shift 2 ;;
        --jvm-gc) gc=${2:?--jvm-gc requires g1, serial or zgc}; shift 2 ;;
        --stress-order) stress=${2:?--stress-order requires an order}; shift 2 ;;
        --jfr) jfr=true; shift ;;
        --prepared) prepared=true; shift ;;
        --output) output=${2:?--output requires a directory}; shift 2 ;;
        --smoke) smoke=true; shift ;;
        --native) native=true; shift ;;
        --scalar) scalar=true; shift ;;
        --quality) mode=quality; shift ;;
        --concurrency) mode=concurrency; shift ;;
        --help) echo 'Usage: benchmark.sh [--output DIR] [--smoke] [--scalar|--native] [--quality|--concurrency] [--repetitions N] [--heap-mib N] [--jvm-gc g1|serial|zgc] [--stress-order N] [--jfr] [-- JMH/Gradle arguments]'; exit 0 ;;
        --) shift; break ;;
        *) break ;;
    esac
done
[[ $repetitions =~ ^[1-9][0-9]*$ && $heap =~ ^[1-9][0-9]*$ ]] || { echo 'Repetitions and heap must be positive integers.' >&2; exit 2; }
[[ -z $stress || $stress =~ ^[1-9][0-9]*$ && $stress -gt 16 ]] || { echo 'Stress order must exceed 16.' >&2; exit 2; }
[[ $jfr == false || $native == false && $mode == performance ]] || { echo 'JFR requires JVM performance.' >&2; exit 2; }
[[ $mode == performance || $# == 0 ]] || { echo 'Benchmark arguments require performance mode.' >&2; exit 2; }
case $gc in g1) gc_arg=-XX:+UseG1GC ;; serial) gc_arg=-XX:+UseSerialGC ;; zgc) gc_arg=-XX:+UseZGC ;; *) echo 'Unsupported JVM GC.' >&2; exit 2 ;; esac
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
if [[ $prepared == true ]]; then
    [[ $native == false ]] || { echo 'Use the prepared Native lab adapter for Native runs.' >&2; exit 2; }
    java=${KUMULANT_BENCH_JAVA:?Prepared mode requires KUMULANT_BENCH_JAVA}
else
    ./gradlew --console=plain :kumulant-bench:prepareBenchmark
    IFS= read -r java < kumulant-bench/build/benchmark-java
fi
jvm_args=(--enable-native-access=ALL-UNNAMED "-Xms${heap}m" "-Xmx${heap}m" "$gc_arg" "-Dbench.smoke=$smoke")
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
cp kumulant-bench/build/benchmark-dependencies.txt "$output/dependencies.txt"
base_output=$output
for ((pass=1; pass<=repetitions; pass++)); do
    if ((repetitions > 1)); then output="$base_output/pass-$pass"; mkdir "$output"; fi
    printf 'status=incomplete\n' > "$output/status.txt"
    "${runner[@]}" --quality > "$output/quality.txt" 2>&1
    expected='backend=simd('
    [[ $scalar == false ]] || expected='backend=scalar/'
    resolved_backend() { [[ $(<"$1") == *"$expected"* ]]; }
    [[ $native == true ]] || resolved_backend "$output/quality.txt" || { echo 'Requested backend did not resolve; inspect quality.txt.' >&2; exit 1; }
    if [[ -n $stress ]]; then
        "${runner[@]}" --cholesky-stress "$stress" > "$output/stress.txt" 2>&1
    fi
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
        profiler=()
        [[ $jfr == false ]] || profiler=(-prof "jfr:dir=$output/jfr")
        "${runner[@]}" "$@" "${profiler[@]}" -rf json -rff "$output/results.json" 2>&1 | tee "$output/benchmark.txt"
    fi
    printf 'status=complete\n' > "$output/status.txt"
done
