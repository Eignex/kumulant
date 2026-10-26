"""Validate supplemental Native capture and regenerate its summaries (stdlib only)."""

import argparse
import csv
import gzip
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--local', action='store_true', help='validate the separate desktop integration run')
options = parser.parse_args()
ROOT = Path(__file__).resolve().parent
if options.local:
    ROOT = ROOT / 'local'


def read(name):
    return gzip.decompress((ROOT / name).read_bytes()).decode()


def write_csv(name, rows):
    with (ROOT / name).open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


if not options.local:
    provenance = json.loads((ROOT / 'provenance.json').read_text())
    job = json.loads(read('lab/job.json.gz'))
    assert job['status'] == 'DONE' and job['sha'] == provenance['source']
    assert (ROOT / 'lab/0.exit').read_text().strip() == '0'
    for entry in json.loads((ROOT / 'lab/inventory.json').read_text()):
        path = ROOT / entry['retained']
        data = gzip.decompress(path.read_bytes()) if path.suffix == '.gz' else path.read_bytes()
        assert len(data) == entry['bytes'] and hashlib.sha256(data).hexdigest() == entry['sha256'], entry['source']

quality = read('native-quality.txt.gz')
identity = [line for line in quality.splitlines() if line.startswith(('Cholesky backend:', 'Cholesky vendor:', 'Cholesky library:', 'Cholesky version:', 'Cholesky threads:'))]
assert any(line.startswith('Cholesky backend: ') for line in identity)
assert any(line.startswith('Cholesky vendor: ') for line in identity)
assert len([line for line in quality.splitlines() if line.startswith('Native first trailing update: ')]) == 6

factor, update, extension, diagonal, retry = [], [], [], [], []
for line in quality.splitlines():
    cells = line.split(',')
    if cells[0] in ['precision', 'rbf', 'matern']:
        factor.append([float(value) for value in cells[3:]])
    elif cells[0] == 'update' and cells[1] != 'size':
        update.append([float(value) for value in cells[3:]])
    elif cells[0] == 'extension' and cells[1] != 'kernel':
        extension.append([float(value) for value in cells[5:]])
    elif cells[0] == 'diagonal' and cells[1] != 'size':
        diagonal.append(float(cells[3]))
    elif cells[0] == 'retry' and cells[1] != 'scenario':
        retry.append(cells[1:])
assert [len(factor), len(update), len(extension), len(diagonal), len(retry)] == [54, 12, 108, 3, 3]
assert retry == [['success', '1', '0.0', 'true'], ['retry', '4', '1.0E-8', 'true'], ['exhaustion', '4', '1.0E-8', 'false']]
assert all(row[0] < 1e-13 and row[1] < 1e-13 for row in factor)
assert all(row[0] < 1e-13 and row[1] < 1e-13 and row[2] < 1e-10 and row[3] < 1e-9 for row in update)
assert all(row[0] < 1e-13 and row[1] < 1e-13 and row[2] < row[4] and row[3] < row[4] for row in extension)
assert max(diagonal) <= 1e-10
accuracy = []
for name, rows in [('factor', factor), ('update', update), ('extension', extension)]:
    accuracy.append({
        'group': name, 'fixtures': len(rows),
        'max_reconstruction': max(row[0] for row in rows),
        'max_backward_error': max(row[1] for row in rows),
        'max_relative_solution_error': max(row[2] for row in rows) if name != 'factor' else '',
        'max_absolute_logdet_error': max(row[3] for row in rows) if name != 'factor' else '',
    })
accuracy.append({
    'group': 'diagonal', 'fixtures': 3, 'max_reconstruction': '', 'max_backward_error': '',
    'max_relative_solution_error': '', 'max_absolute_logdet_error': max(diagonal),
})
write_csv('accuracy.csv', accuracy)

timings = []
for repetition in range(2):
    results = json.loads(read(f'native-{repetition}.json.gz'))
    log = read(f'native-{repetition}-benchmark.txt.gz')
    assert len(results) == 4
    for line in identity:
        assert log.splitlines().count(line) >= len(results), (repetition, line)
    assert {(row['benchmark'].rsplit('.', 1)[-1], row['params']['matrixCase']) for row in results} == {
        (method, fixture) for method in ['factor', 'factorWithWorkspace'] for fixture in ['precision:32', 'rbf:512']
    }
    for row in results:
        assert row['compilationMode'] == 'Release' and row['mode'] == 'avgt'
        assert row['warmupIterations'] == 3 and row['measurementIterations'] == 5
        assert row['warmupTime'] == row['measurementTime'] == '1000 ms'
        metric = row['primaryMetric']
        assert metric['scoreUnit'] == 'us/op' and list(map(len, metric['rawData'])) == [5]
        timings.append({
            'process_repetition': repetition,
            'benchmark': row['benchmark'].rsplit('.', 1)[-1], 'matrixCase': row['params']['matrixCase'],
            'us_per_op': metric['score'], 'engine_score_error': metric['scoreError'],
            'samples': 5,
        })
write_csv('measurements.csv', timings)
print('Validated 54 factors, 12 update sequences, 108 extensions, 3 determinants, 3 retry outcomes, and 8 Native timing rows.')
