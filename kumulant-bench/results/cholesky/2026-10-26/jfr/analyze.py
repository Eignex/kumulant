"""Attribute double-array samples by their recorded allocation stacks, not exact B/op."""

import csv
import gzip
import hashlib
import json
from collections import defaultdict
from pathlib import Path

root = Path(__file__).resolve().parent
rows = []
for recording in json.loads((root / "recordings.json").read_text()):
    data = (root / recording["retained_file"]).read_bytes()
    assert hashlib.sha256(data).hexdigest() == recording["retained_sha256"]
    events = json.loads(gzip.decompress(data))["recording"]["events"]
    totals = defaultdict(lambda: [0, 0])
    for event in events:
        values = event["values"]
        if values["objectClass"]["name"] not in ["[D", "double[]"]:
            continue
        frames = (values.get("stackTrace") or {}).get("frames", [])
        names = [frame["method"]["type"]["name"] + "." + frame["method"]["name"] for frame in frames]
        if any(name.startswith(("com/eignex/koblas/dense/", "com/eignex/koblas/internal/jvm25/")) for name in names):
            category = "backend"
        elif any("CholeskyKt.updateCholeskyTrailing" in name for name in names):
            category = "panel-and-trailing-copy"
        elif any("CholeskyFactorKt.factorCholeskyWithWorkspace" in name for name in names):
            category = "owned-factor"
        else:
            category = "other"
        totals[category][0] += values["weight"]
        totals[category][1] += 1
    for category, (weight, count) in totals.items():
        rows.append({
            "backend": recording["backend"],
            "operation": recording["operation"],
            "fixture": recording["fixture"],
            "category": category,
            "sample_weight_bytes": weight,
            "samples": count,
        })
with (root / "allocation-sites.csv").open("w", newline="") as stream:
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
