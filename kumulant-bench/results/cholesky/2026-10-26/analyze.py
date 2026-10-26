"""Regenerate comparison tables from the retained engine reports (Python standard library)."""

import csv
import gzip
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def key(result):
    return result["benchmark"], tuple(sorted(result.get("params", {}).items()))


def metric(result):
    primary = result["primaryMetric"]
    secondary = result.get("secondaryMetrics", {})
    return {
        "us_per_op": primary["score"],
        "engine_score_error": primary.get("scoreError", ""),
        "bytes_per_op": secondary.get("gc.alloc.rate.norm", {}).get("score", ""),
        "gc_count": secondary.get("gc.count", {}).get("score", ""),
        "gc_time_ms": secondary.get("gc.time", {}).get("score", ""),
    }


def write_csv(name, rows):
    if rows:
        with (ROOT / name).open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)


reports = {
    path.name.removesuffix(".json.gz"): json.loads(gzip.decompress(path.read_bytes()))
    for path in sorted((ROOT / "raw").glob("*.json.gz"))
}
for capture, results in reports.items():
    for row in results:
        row["capture"] = capture


def current_candidate(backend):
    rows = list(reports.get("candidate-" + backend, []))
    rows += reports.get("candidate-batches-" + backend, [])
    rows += reports.get("final-solves-" + backend, [])
    return list({key(row): row for row in rows}.values())


all_rows = []
for name, results in reports.items():
    for result in results:
        assert result["primaryMetric"]["scoreUnit"] == "us/op"
        all_rows.append({
            "capture": name,
            "benchmark": result["benchmark"].removeprefix("com.eignex.kumulant.bench."),
            "parameters": json.dumps(result.get("params", {}), sort_keys=True),
            **metric(result),
        })
write_csv("measurements.csv", all_rows)

comparisons = []
for backend in ["scalar", "simd", "native-0", "native-1"]:
    before = {key(r): r for r in reports.get("baseline-" + backend, [])}
    after = current_candidate(backend)
    for result in after:
        old = before.get(key(result))
        if old is None:
            continue
        a, b = metric(old), metric(result)
        comparisons.append({
            "backend": backend,
            "benchmark": result["benchmark"].removeprefix("com.eignex.kumulant.bench."),
            "parameters": json.dumps(result.get("params", {}), sort_keys=True),
            "baseline_capture": old["capture"],
            "candidate_capture": result["capture"],
            "baseline_us": a["us_per_op"],
            "candidate_us": b["us_per_op"],
            "candidate_over_baseline": b["us_per_op"] / a["us_per_op"],
            "baseline_bytes": a["bytes_per_op"],
            "candidate_bytes": b["bytes_per_op"],
        })
write_csv("before-after.csv", comparisons)

alternatives = {
    "factorWithWorkspace": "factor",
    "solveMatrixInto": "solveMatrix",
    "solveLowerInto": "solveLower",
    "solveVectorInto": "solveVector",
    "candidateBatchesInto": "candidateBatches",
    "factorSolveLogDeterminantWithWorkspace": "factorSolveLogDeterminant",
    "fittingRoundWithWorkspace": "fittingRound",
    "rankUpdateWithWorkspace": "refactorUpdate",
    "posteriorUpdateSolve": "posteriorRefactorSolve",
    "extendWithWorkspace": "refactor",
}
pairs = []
for capture, results in reports.items():
    if not capture.startswith(("candidate", "final-solves")):
        continue
    lookup = {key(r): r for r in results}
    for result in results:
        class_name, method = result["benchmark"].rsplit(".", 1)
        if method not in alternatives:
            continue
        other = lookup[(class_name + "." + alternatives[method], key(result)[1])]
        a, b = metric(other), metric(result)
        pairs.append({
            "capture": capture,
            "benchmark": class_name.removeprefix("com.eignex.kumulant.bench.") + "." + method,
            "reference_method": alternatives[method],
            "parameters": json.dumps(result.get("params", {}), sort_keys=True),
            "reference_us": a["us_per_op"],
            "alternative_us": b["us_per_op"],
            "reference_over_alternative": a["us_per_op"] / b["us_per_op"],
            "reference_bytes": a["bytes_per_op"],
            "alternative_bytes": b["bytes_per_op"],
        })
write_csv("alternatives.csv", pairs)

consumer = []
for name in ["models", "search"]:
    with (ROOT / "consumer" / ("baseline-" + name + ".csv")).open() as stream:
        before = list(csv.DictReader(stream))
    with (ROOT / "consumer" / ("candidate-" + name + ".csv")).open() as stream:
        after = list(csv.DictReader(stream))
    assert len(before) == len(after)
    maximum = 0.0
    for old, new in zip(before, after):
        assert old.keys() == new.keys()
        for field in old:
            if old[field] == new[field]:
                continue
            delta = abs(float(old[field]) - float(new[field]))
            assert delta <= 1e-10, (name, field, delta)
            maximum = max(maximum, delta)
    consumer.append({"report": name, "rows": len(before), "maximum_absolute_delta": maximum})
write_csv("consumer-parity.csv", consumer)

accuracy = []
for location, backend, path in [
    (location, backend, ROOT / "logs" / directory / (backend + "-quality.txt"))
    for location, directory in [("local", "final-quality"), ("lab", "final-solves")]
    for backend in ["scalar", "simd"]
]:
    lines = path.read_text().splitlines()
    groups = {"factor": [], "update": [], "extension": [], "diagonal": []}
    for line in lines:
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
        assert rows
        maximum = [max(column) for column in zip(*rows)]
        accuracy.append({
            "location": location,
            "backend": backend,
            "group": group,
            "fixtures": len(rows),
            "max_reconstruction": maximum[0] if group != "diagonal" else "",
            "max_backward_error": maximum[1] if len(maximum) > 1 else "",
            "max_relative_solution_error": maximum[2] if len(maximum) > 2 else "",
            "max_absolute_logdet_error": maximum[3] if len(maximum) > 3 else (maximum[0] if group == "diagonal" else ""),
        })
write_csv("accuracy.csv", accuracy)

payloads = []
for n in [128, 512, 1024]:
    block = min(64, n // 4)
    heights = [n - start - min(block, n - start) for start in range(0, n, block)]
    payloads.append({
        "order": n,
        "owned_factor_bytes": 8 * n * n,
        "panel_bytes": sum(8 * height * block for height in heights),
        "trailing_bytes": sum(8 * height * height for height in heights),
    })
write_csv("caller-payloads.csv", payloads)


def result(capture, class_name, method, parameter):
    results = current_candidate(capture.removeprefix("current-candidate-")) if capture.startswith("current-candidate-") else reports.get(capture, [])
    matches = [
        r for r in results
        if r["benchmark"].endswith(class_name + "." + method)
        and parameter in r.get("params", {}).values()
    ]
    return matches[0] if matches else None


def time_text(row):
    p = row["primaryMetric"]
    return f'{p["score"]:,.3f} ± {p["scoreError"]:,.3f}'


def bytes_text(row):
    value = metric(row)["bytes_per_op"]
    return "—" if value == "" else ("<1" if value < 1 else f"{value:,.0f}")


table = [
    "# Selected measurements",
    "",
    "Generated by `python3 analyze.py`. All times are microseconds per complete benchmark invocation.",
    "JVM ± values are JMH score errors; raw JSON retains confidence intervals and every fork sample.",
    "Native ranges cover the two separate process means, rather than a confidence interval.",
    "Solve comparisons use the final matched captures; other alternatives retain their original matched candidate capture.",
    "",
]
for backend in ["scalar", "simd"]:
    table += [
        "## JVM " + backend + " before/after", "",
        "| Operation and fixture | Baseline µs/op | Candidate µs/op | Baseline B/op | Candidate B/op |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for cls, method, parameter in [
        ("CholeskyFactorBenchmark", "factor", "precision:8"),
        ("CholeskyFactorBenchmark", "factor", "rbf:128"),
        ("CholeskyFactorBenchmark", "factor", "rbf:512"),
        ("CholeskySolveBenchmark", "solveMatrix", "32x1"),
        ("CholeskySolveBenchmark", "solveMatrix", "512x64"),
        ("CholeskyWorkloadBenchmark", "fittingRound", "128"),
    ]:
        a = result("baseline-" + backend, cls, method, parameter)
        b = result("current-candidate-" + backend, cls, method, parameter)
        if a and b:
            table.append(f"| {method} {parameter} | {time_text(a)} | {time_text(b)} | {bytes_text(a)} | {bytes_text(b)} |")
    table += [
        "", "## JVM " + backend + " alternatives within the candidate", "",
        "| Alternative / reference and fixture | Reference µs/op | Alternative µs/op | Reference B/op | Alternative B/op |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for cls, method, parameter in [
        ("CholeskyFactorBenchmark", "factorWithWorkspace", "precision:8"),
        ("CholeskyFactorBenchmark", "factorWithWorkspace", "rbf:128"),
        ("CholeskyFactorBenchmark", "factorWithWorkspace", "rbf:512"),
        ("CholeskySolveBenchmark", "solveMatrixInto", "512x64"),
        ("CholeskyUpdateBenchmark", "rankUpdateWithWorkspace", "8"),
        ("CholeskyUpdateBenchmark", "rankUpdateWithWorkspace", "512"),
        ("CholeskyUpdateBenchmark", "posteriorUpdateSolve", "512"),
        ("CholeskyExtensionBenchmark", "extendWithWorkspace", "16x1"),
        ("CholeskyExtensionBenchmark", "extendWithWorkspace", "512x16"),
        ("CholeskyWorkloadBenchmark", "fittingRoundWithWorkspace", "128"),
    ]:
        capture = ("final-solves-" if cls == "CholeskySolveBenchmark" and "final-solves-" + backend in reports else "candidate-") + backend
        a = result(capture, cls, alternatives[method], parameter)
        b = result(capture, cls, method, parameter)
        if a and b:
            table.append(f"| {method} / {alternatives[method]} {parameter} | {time_text(a)} | {time_text(b)} | {bytes_text(a)} | {bytes_text(b)} |")
    table.append("")

table += [
    "## Linux x64 process means", "",
    "| Operation and fixture | Initial baseline µs/op | Repeated baseline µs/op | Candidate µs/op |",
    "| --- | ---: | ---: | ---: |",
]
for cls, method, parameter in [
    ("CholeskyFactorBenchmark", "factor", "precision:8"),
    ("CholeskyFactorBenchmark", "factor", "rbf:128"),
    ("CholeskyFactorBenchmark", "factor", "rbf:512"),
    ("CholeskySolveBenchmark", "solveMatrix", "32x1"),
    ("CholeskySolveBenchmark", "solveMatrix", "512x64"),
    ("CholeskyVectorBenchmark", "solveVector", "128"),
]:
    cells = []
    for capture in ["baseline-native", "repeat-baseline-native", "current-candidate-native"]:
        rows = [result(capture + "-" + str(i), cls, method, parameter) for i in range(2)]
        scores = [r["primaryMetric"]["score"] for r in rows if r]
        cells.append(f"{min(scores):,.3f}–{max(scores):,.3f}" if len(scores) == 2 else "—")
    table.append(f"| {method} {parameter} | " + " | ".join(cells) + " |")
table += [
    "", "## Linux x64 alternatives", "",
    "| Alternative / reference and fixture | Reference µs/op | Alternative µs/op |",
    "| --- | ---: | ---: |",
]
for cls, method, parameter in [
    ("CholeskyFactorBenchmark", "factorWithWorkspace", "rbf:128"),
    ("CholeskyFactorBenchmark", "factorWithWorkspace", "rbf:512"),
    ("CholeskySolveBenchmark", "solveMatrixInto", "512x64"),
    ("CholeskyUpdateBenchmark", "rankUpdateWithWorkspace", "8"),
    ("CholeskyUpdateBenchmark", "rankUpdateWithWorkspace", "512"),
    ("CholeskyUpdateBenchmark", "posteriorUpdateSolve", "512"),
    ("CholeskyExtensionBenchmark", "extendWithWorkspace", "16x1"),
    ("CholeskyExtensionBenchmark", "extendWithWorkspace", "512x16"),
]:
    cells = []
    for operation in [alternatives[method], method]:
        capture = "final-solves-native-" if cls == "CholeskySolveBenchmark" and "final-solves-native-0" in reports else "candidate-native-"
        rows = [result(capture + str(i), cls, operation, parameter) for i in range(2)]
        scores = [r["primaryMetric"]["score"] for r in rows if r]
        cells.append(f"{min(scores):,.3f}–{max(scores):,.3f}" if len(scores) == 2 else "—")
    table.append(f"| {method} / {alternatives[method]} {parameter} | " + " | ".join(cells) + " |")
table += [
    "", "## Contemporary small JVM repeats", "",
    "These use the intermediate single-RHS allocating route at `63831d6`. Factor code is unchanged; the solve regression prompted restoring the original route at `1958455`.", "",
    "| Backend, operation, fixture | Repeated baseline µs/op | Repeated candidate µs/op |",
    "| --- | ---: | ---: |",
]
for backend in ["scalar", "simd"]:
    for cls, method, parameter in [
        ("CholeskyFactorBenchmark", "factor", "precision:8"),
        ("CholeskySolveBenchmark", "solveMatrix", "32x1"),
    ]:
        a = result("small-baseline-" + backend, cls, method, parameter)
        b = result("small-candidate-" + backend, cls, method, parameter)
        if a and b:
            table.append(f"| {backend} {method} {parameter} | {time_text(a)} | {time_text(b)} |")
(ROOT / "tables.md").write_text("\n".join(table) + "\n")
