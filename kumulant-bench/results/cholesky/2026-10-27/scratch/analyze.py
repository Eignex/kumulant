"""Regenerate before-after.csv and tables.md from the retained JVM captures.

Refuses to publish a matched table unless both jobs ended with zero exit status on the same
Lab host, CPU model, JDK and resource budget, with identical benchmark selections.
"""
import csv
import gzip
import json
import pathlib
import statistics

HERE = pathlib.Path(__file__).parent
JOBS = {"baseline": "8718", "candidate": "8719"}
BACKENDS = ("scalar", "simd")


def job_receipt(job):
    directory = HERE / "lab" / job
    record = json.loads(gzip.open(directory / "lab-cases.jsonl.gz", "rt").readline())
    resources = json.loads((directory / "resources.json").read_text())
    cpu = next(line.split(":", 1)[1].strip() for line in (directory / "host.txt").read_text().splitlines()
               if line.startswith("Model name"))
    return {
        "exit": (directory / "0.exit").read_text().strip(),
        "host": record["host"],
        "cpu": cpu,
        "jdk": resources["javaVersion"],
        "budget": json.dumps(resources["budget"], sort_keys=True),
        "enforcement": resources["enforcement"],
        "oom": "oom_kill 0" in resources["counters"]["memory.events"],
    }


def load(arm, backend):
    rows = {}
    for result in json.load(gzip.open(HERE / "raw" / f"{arm}-{backend}.json.gz", "rt")):
        key = (result["benchmark"].rsplit(".", 2)[-2] + "." + result["benchmark"].rsplit(".", 1)[-1],
               json.dumps(result.get("params", {}), sort_keys=True))
        samples = [value for fork in result["primaryMetric"]["rawData"] for value in fork]
        secondary = result["secondaryMetrics"]
        rows[key] = {
            "us": result["primaryMetric"]["score"],
            "error": result["primaryMetric"]["scoreError"],
            "samples": samples,
            "forks": len(result["primaryMetric"]["rawData"]),
            "bytes": secondary["gc.alloc.rate.norm"]["score"],
            "gc_count": secondary.get("gc.count", {}).get("score", 0.0),
            "gc_ms": secondary.get("gc.time", {}).get("score", 0.0),
            "warmup": result["warmupIterations"],
            "measurement": result["measurementIterations"],
        }
    return rows


def main():
    receipts = {arm: job_receipt(job) for arm, job in JOBS.items()}
    for field in ("host", "cpu", "jdk", "budget", "enforcement"):
        values = {receipt[field] for receipt in receipts.values()}
        if len(values) != 1:
            raise SystemExit(f"unpaired {field}: {values}")
    if any(receipt["exit"] != "0" or not receipt["oom"] for receipt in receipts.values()):
        raise SystemExit(f"incomplete capture: {receipts}")
    lines = []
    table = ["| backend | benchmark | parameters | baseline µs | candidate µs | ratio | baseline B/op | candidate B/op |",
             "|---|---|---|---|---|---|---|---|"]
    for backend in BACKENDS:
        before, after = load("baseline", backend), load("candidate", backend)
        if before.keys() != after.keys():
            raise SystemExit(f"{backend}: benchmark selections differ")
        for key in sorted(before):
            b, a = before[key], after[key]
            if (b["forks"], b["warmup"], b["measurement"]) != (a["forks"], a["warmup"], a["measurement"]):
                raise SystemExit(f"{backend} {key}: protocol differs")
            ratio = a["us"] / b["us"]
            lines.append({
                "backend": backend, "benchmark": key[0], "parameters": key[1],
                "baseline_us": b["us"], "baseline_error": b["error"],
                "baseline_sample_sd": statistics.stdev(b["samples"]),
                "candidate_us": a["us"], "candidate_error": a["error"],
                "candidate_sample_sd": statistics.stdev(a["samples"]),
                "candidate_over_baseline": ratio,
                "intervals_overlap": abs(a["us"] - b["us"]) <= a["error"] + b["error"],
                "baseline_bytes": b["bytes"], "candidate_bytes": a["bytes"],
                "baseline_gc_count": b["gc_count"], "candidate_gc_count": a["gc_count"],
                "baseline_gc_ms": b["gc_ms"], "candidate_gc_ms": a["gc_ms"],
                "samples_per_arm": len(b["samples"]),
            })
            table.append(
                f"| {backend} | {key[0]} | {', '.join(f'{k}={v}' for k, v in json.loads(key[1]).items())} "
                f"| {b['us']:.2f} ± {b['error']:.2f} | {a['us']:.2f} ± {a['error']:.2f} | {ratio:.3f} "
                f"| {b['bytes']:.0f} | {a['bytes']:.0f} |"
            )
    with open(HERE / "before-after.csv", "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(lines[0]))
        writer.writeheader()
        writer.writerows(lines)
    receipt = receipts["baseline"]
    header = (f"Host {receipt['host']}, {receipt['cpu']}, JDK {receipt['jdk']}, budget {receipt['budget']} "
              f"({receipt['enforcement']}). Mean ± JMH score error (99.9%); ratio is candidate over baseline.\n\n")
    (HERE / "tables.md").write_text(header + "\n".join(table) + "\n")


if __name__ == "__main__":
    main()
