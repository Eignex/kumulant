#!/usr/bin/env bash
set -euo pipefail
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
: "${OUT:?Eignex Lab supplies the retained output directory}"
mkdir -p "$OUT/report"
for backend in scalar simd; do
    options=()
    [[ "$backend" != scalar ]] || options+=(--scalar)
    kumulant-bench/benchmark.sh --output "$OUT/report/$backend" "${options[@]}" -- \
        '.*Cholesky.*Benchmark.*' -bm avgt -tu us -prof gc "$@"
done
