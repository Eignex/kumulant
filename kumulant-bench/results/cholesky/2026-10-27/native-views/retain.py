"""Retain one fetched Lab job under lab/<label>/ and <label>/ with a byte/hash inventory (stdlib only).

Usage: python3 -I retain.py <label> <fetched job directory> <job.json> <cases.jsonl>
"""

import gzip
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
label, fetched, job_file, cases_file = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4])
lab, report = ROOT / 'lab' / label, ROOT / label
lab.mkdir(parents=True, exist_ok=True)
report.mkdir(exist_ok=True)

# Preparation archives and the driver JVM's recording stay in Lab: the archive is a build input, and
# the parent-process JFR measures neither JMH forks nor Native children.
SKIPPED_SUFFIXES = ('.zip', '.jfr', '.lock')


def store(data, target):
    if target.suffix in ('.out', '.err') or (target.name != 'host.txt' and len(data) > 4096 and target.suffix in ('.txt', '.json', '.log', '.jsonl')):
        target = target.with_name(target.name + '.gz')
        target.write_bytes(gzip.compress(data, mtime=0))
    else:
        target.write_bytes(data)
    return target


inventory = []
files = sorted(path for path in fetched.rglob('*') if path.is_file())
listing = []
for path in files:
    source = path.relative_to(fetched).as_posix()
    listing.append(f'{path.stat().st_size}\t{source}')
    if source.endswith(SKIPPED_SUFFIXES) or '/live/' in source or source.startswith('lab-'):
        continue
    data = path.read_bytes()
    if source.startswith('cases/0/out/report/'):
        relative = source.removeprefix('cases/0/out/report/')
        if '/cases/' in relative or '/samples-' in relative or relative.endswith('progress.txt'):
            # Per-case Native protocol files are folded into samples.txt and the engine JSON.
            continue
        target = report / relative.replace('/', '-') if relative.startswith('native-') and '/' in relative else report / relative
    else:
        target = lab / source.replace('/', '-')
    target.parent.mkdir(parents=True, exist_ok=True)
    retained = store(data, target)
    inventory.append({'source': source, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                      'retained': retained.relative_to(ROOT).as_posix()})
(lab / 'files.txt').write_text('\n'.join(listing) + '\n')
(lab / 'inventory.json').write_text(json.dumps(inventory, indent=2) + '\n')
store(job_file.read_bytes(), lab / 'job.json')
(lab / 'cases.json').write_text(json.dumps([json.loads(line) for line in cases_file.read_text().splitlines() if line], indent=2) + '\n')
print(f'Retained {len(inventory)} of {len(files)} files for {label}')
