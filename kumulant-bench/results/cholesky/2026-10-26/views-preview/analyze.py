"""Compare allocations in the exploratory source-pinned copy/view capture."""

import csv
import gzip
import json
from pathlib import Path

root = Path(__file__).resolve().parent
rows = []
for backend in ["scalar", "simd"]:
    reports = {
        path: json.loads(gzip.decompress((root / (backend + "-" + path + ".json.gz")).read_bytes()))
        for path in ["copy", "view"]
    }
    baseline = {(r["benchmark"], r["params"]["matrixCase"]): r for r in reports["copy"]}
    for result in reports["view"]:
        old = baseline[result["benchmark"], result["params"]["matrixCase"]]
        rows.append({
            "backend": backend,
            "method": result["benchmark"].rsplit(".", 1)[-1],
            "fixture": result["params"]["matrixCase"],
            "copy_bytes_per_op": old["secondaryMetrics"]["gc.alloc.rate.norm"]["score"],
            "view_bytes_per_op": result["secondaryMetrics"]["gc.alloc.rate.norm"]["score"],
        })
with (root / "allocations.csv").open("w", newline="") as stream:
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
