"""Regenerate matched copying/view tables from retained engine JSON (standard library)."""

import csv
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def key(result):
    return result["benchmark"], tuple(sorted(result.get("params", {}).items()))


def write_csv(name, rows):
    if not rows:
        return
    with (ROOT / name).open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


required = [
    ROOT / "raw" / (arm + suffix + "-" + backend + ".json.gz")
    for arm in ["copy", "view"]
    for suffix in ["-precision", "-rbf", "-gp", "-small"]
    for backend in ["scalar", "simd", "native-0", "native-1"]
]
missing = [path.name for path in required if not path.exists()]
if missing and "--available" not in sys.argv:
    raise SystemExit("Lab captures pending; use --available for partial tables: " + ", ".join(missing))
(ROOT / "capture-status.json").write_text(json.dumps({"complete": not missing, "missing": missing}, indent=2) + "\n")
comparisons = []
for backend in ["scalar", "simd", "native-0", "native-1"]:
    reports = {}
    for arm in ["copy", "view"]:
        results = []
        for suffix, count in [("-precision", 6), ("-rbf", 6), ("-gp", 8), ("-small", 2)]:
            native = ""
            path = ROOT / "raw" / (arm + suffix + native + "-" + backend + ".json.gz")
            pair = ROOT / "raw" / (("view" if arm == "copy" else "copy") + suffix + native + "-" + backend + ".json.gz")
            if not path.exists() or not pair.exists():
                continue
            capture = json.loads(gzip.decompress(path.read_bytes()))
            assert len(capture) == count, (path, len(capture))
            for row in capture:
                row["capture"] = path.name
            results.extend(capture)
        reports[arm] = {key(row): row for row in results}
        assert len(reports[arm]) == len(results), (backend, arm, len(results))
        if not missing:
            assert len(results) == 22, (backend, arm, len(results))
    assert reports["copy"].keys() == reports["view"].keys(), backend
    for identity, old in reports["copy"].items():
        new = reports["view"][identity]
        before, after = old["primaryMetric"], new["primaryMetric"]
        assert before["scoreUnit"] == after["scoreUnit"] == "us/op"
        comparisons.append({
            "backend": backend,
            "benchmark": old["benchmark"].removeprefix("com.eignex.kumulant.bench."),
            "parameters": json.dumps(old.get("params", {}), sort_keys=True),
            "production_route": "unblocked" if old.get("params", {}).get("matrixCase") in ["precision:8", "precision:32"] else ("copy" if backend.startswith("native") else "view"),
            "copy_capture": old["capture"],
            "view_capture": new["capture"],
            "copy_us": before["score"],
            "copy_engine_error": before.get("scoreError", ""),
            "view_us": after["score"],
            "view_engine_error": after.get("scoreError", ""),
            "view_over_copy": after["score"] / before["score"],
            "copy_bytes": old.get("secondaryMetrics", {}).get("gc.alloc.rate.norm", {}).get("score", ""),
            "view_bytes": new.get("secondaryMetrics", {}).get("gc.alloc.rate.norm", {}).get("score", ""),
        })
write_csv("before-after.csv", comparisons)

lines = [
    "# Matched copying/view measurements",
    "",
    "JVM uncertainty is the JMH score error from two forks; Native rows retain each",
    "process separately with its engine error. Ratios below one favor views. Small",
    "differences within uncertainty do not establish a performance change.",
    "Native view rows describe the rejected trial; production retains the copying route.",
    "",
]
for backend in ["scalar", "simd", "native-0", "native-1"]:
    lines += [
        "## " + backend, "",
        "| Method and fixture | Copy µs/op | View µs/op | View/copy | Copy B/op | View B/op |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in comparisons:
        if row["backend"] != backend:
            continue
        fixture = ", ".join(str(v) for v in json.loads(row["parameters"]).values())
        label = row["benchmark"].rsplit(".", 1)[-1] + " " + fixture
        before = f'{row["copy_us"]:.3f} ± {row["copy_engine_error"]:.3f}'
        after = f'{row["view_us"]:.3f} ± {row["view_engine_error"]:.3f}'
        allocation = ["—" if row[name] == "" else f'{row[name]:.0f}' for name in ["copy_bytes", "view_bytes"]]
        lines.append(f'| {label} | {before} | {after} | {row["view_over_copy"]:.3f} | {allocation[0]} | {allocation[1]} |')
    lines.append("")
(ROOT / "tables.md").write_text("\n".join(lines))

consumer = []
for kind in ["models", "search"]:
    with (ROOT / "consumer" / ("published-baseline-" + kind + ".csv")).open() as stream:
        before = list(csv.DictReader(stream))
    with (ROOT / "consumer" / ("view-" + kind + ".csv")).open() as stream:
        after = list(csv.DictReader(stream))
    assert len(before) == len(after)
    maximum = 0.0
    for old, new in zip(before, after):
        assert old.keys() == new.keys()
        for field in old:
            if old[field] != new[field]:
                delta = abs(float(old[field]) - float(new[field]))
                assert delta <= 1e-10, (kind, field, delta)
                maximum = max(maximum, delta)
    consumer.append({"report": kind, "rows": len(before), "maximum_absolute_delta": maximum})
write_csv("consumer-parity.csv", consumer)

accuracy = []
paths = list((ROOT / "validation").glob("*-quality.txt")) + list((ROOT / "logs").glob("*-quality.txt.gz"))
for path in sorted(paths):
    groups = {"factor": [], "update": [], "extension": [], "diagonal": []}
    content = gzip.decompress(path.read_bytes()).decode() if path.suffix == ".gz" else path.read_text()
    for line in content.splitlines():
        row = line.split(",")
        if row[0] in ["precision", "rbf", "matern"] and len(row) == 5:
            groups["factor"].append([float(x) for x in row[3:]])
        elif row[0] == "update" and len(row) == 7 and row[1] != "size":
            groups["update"].append([float(x) for x in row[3:]])
        elif row[0] == "extension" and len(row) == 10 and row[1] != "kernel":
            groups["extension"].append([float(x) for x in row[5:9]])
        elif row[0] == "diagonal" and row[1] != "size":
            groups["diagonal"].append([float(row[-1])])
    for group, rows in groups.items():
        assert rows, (path, group)
        maximum = [max(column) for column in zip(*rows)]
        accuracy.append({
            "capture": path.relative_to(ROOT).as_posix(),
            "group": group,
            "fixtures": len(rows),
            "max_reconstruction": maximum[0] if group != "diagonal" else "",
            "max_backward_error": maximum[1] if len(maximum) > 1 else "",
            "max_relative_solution_error": maximum[2] if len(maximum) > 2 else "",
            "max_absolute_logdet_error": maximum[3] if len(maximum) > 3 else (maximum[0] if group == "diagonal" else ""),
        })
write_csv("accuracy.csv", accuracy)
