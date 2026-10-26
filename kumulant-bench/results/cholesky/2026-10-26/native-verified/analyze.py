"""Validate backend-identified Native arms and regenerate matched tables (stdlib only)."""

import csv
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROVENANCE = json.loads((ROOT / 'provenance.json').read_text())


def read(path):
    source = ROOT / path
    return gzip.decompress(source.read_bytes()).decode() if source.suffix == '.gz' else source.read_text()


def csv_file(name, rows):
    with (ROOT / name).open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def identity(text):
    prefixes = ('Cholesky backend:', 'Cholesky vendor:', 'Cholesky library:', 'Cholesky version:', 'Cholesky threads:')
    lines = [line for line in text.splitlines() if line.startswith(prefixes)]
    assert len(lines) == 5 and '1-thread-confirmed' in lines[-1]
    return lines


def accuracy(arm, text):
    groups = {'factor': [], 'update': [], 'extension': [], 'diagonal': [], 'retry': []}
    for line in text.splitlines():
        cells = line.split(',')
        if cells[0] in ('precision', 'rbf', 'matern'):
            groups['factor'].append(list(map(float, cells[3:])))
        elif cells[0] == 'update' and cells[1] != 'size':
            groups['update'].append(list(map(float, cells[3:])))
        elif cells[0] == 'extension' and cells[1] != 'kernel':
            groups['extension'].append(list(map(float, cells[5:])))
        elif cells[0] == 'diagonal' and cells[1] != 'size':
            groups['diagonal'].append(float(cells[3]))
        elif cells[0] == 'retry' and cells[1] != 'scenario':
            groups['retry'].append(cells[1:])
    assert {name: len(rows) for name, rows in groups.items()} == {
        'factor': 54, 'update': 12, 'extension': 108, 'diagonal': 3, 'retry': 3,
    }
    assert all(row[0] < 1e-13 and row[1] < 1e-13 for row in groups['factor'])
    assert all(row[0] < 1e-13 and row[1] < 1e-13 and row[2] < 1e-10 and row[3] < 1e-9 for row in groups['update'])
    assert all(row[0] < 1e-13 and row[1] < 1e-13 and row[2] < row[4] and row[3] < row[4] for row in groups['extension'])
    assert max(groups['diagonal']) <= 1e-10
    assert groups['retry'] == [['success', '1', '0.0', 'true'], ['retry', '4', '1.0E-8', 'true'], ['exhaustion', '4', '1.0E-8', 'false']]
    rows = []
    for name in ('factor', 'update', 'extension'):
        values = groups[name]
        rows.append({
            'arm': arm, 'group': name, 'fixtures': len(values),
            'max_reconstruction': max(row[0] for row in values),
            'max_backward_error': max(row[1] for row in values),
            'max_relative_solution_error': max(row[2] for row in values) if name != 'factor' else '',
            'max_absolute_logdet_error': max(row[3] for row in values) if name != 'factor' else '',
        })
    rows.append({'arm': arm, 'group': 'diagonal', 'fixtures': 3, 'max_reconstruction': '', 'max_backward_error': '',
                 'max_relative_solution_error': '', 'max_absolute_logdet_error': max(groups['diagonal'])})
    return rows


factor_cases = {
    ('CholeskyFactorBenchmark.' + method, (('matrixCase', fixture),))
    for method in ('factor', 'factorWithWorkspace')
    for fixture in ('precision:32', 'precision:128', 'rbf:512', 'rbf:1024')
}
workload_cases = {
    ('CholeskyWorkloadBenchmark.' + method, (('historySize', '512'),))
    for method in ('factorSolveLogDeterminant', 'factorSolveLogDeterminantWithWorkspace', 'fittingRound', 'fittingRoundWithWorkspace')
}
expected = factor_cases | workload_cases
arms, backends, machines, resources, definitions, hosts, accuracies = {}, {}, {}, {}, {}, {}, []
for arm in ('copy', 'view'):
    job = json.loads(read(f'lab/{arm}/job.json.gz'))
    assert job['status'] == 'DONE' and job['sha'] == PROVENANCE[arm]
    assert read(f'lab/{arm}/0.exit').strip() == '0'
    definitions[arm] = json.loads(read(f'lab/{arm}/definition.json'))
    cases = json.loads(read(f'lab/{arm}/cases.json'))
    assert len(cases) == 1 and cases[0]['status'] == 'DONE'
    hosts[arm] = cases[0]['host']
    assert hosts[arm]
    for entry in json.loads(read(f'lab/{arm}/inventory.json')):
        source = ROOT / entry['retained']
        data = gzip.decompress(source.read_bytes()) if source.suffix == '.gz' else source.read_bytes()
        assert len(data) == entry['bytes'] and hashlib.sha256(data).hexdigest() == entry['sha256'], entry['source']
    quality = read(f'{arm}/native-quality.txt.gz')
    backends[arm] = identity(quality)
    accuracies.extend(accuracy(arm, quality))
    host = dict(line.split(':', 1) for line in read(f'{arm}/host.txt').splitlines() if ':' in line)
    machines[arm] = {key: host[key].strip() for key in ('Architecture', 'CPU(s)', 'Model name', 'Thread(s) per core', 'Core(s) per socket', 'Socket(s)')}
    resource = json.loads(read(f'lab/{arm}/accepted-resources.json'))
    resources[arm] = {key: resource[key] for key in ('enforcement', 'budget', 'cpuMax', 'memoryMax', 'javaVersion', 'os', 'arch')}
    assert resources[arm]['budget'] == {'cpuMillis': 1000, 'memoryMiB': 4608}
    assert resources[arm]['enforcement'] == 'cgroup-v2-hard'
    arms[arm] = {}
    for repetition in range(2):
        results = json.loads(read(f'{arm}/native-{repetition}.json.gz'))
        log = read(f'{arm}/native-{repetition}-benchmark.txt.gz')
        assert len(results) == len(expected)
        assert all(log.splitlines().count(line) >= len(results) for line in backends[arm])
        resolved = {}
        for result in results:
            key = (result['benchmark'].removeprefix('com.eignex.kumulant.bench.'), tuple(sorted(result['params'].items())))
            assert key not in resolved
            assert result['mode'] == 'avgt' and result['compilationMode'] == 'Release'
            assert result['warmupIterations'] == 3 and result['measurementIterations'] == 5
            assert result['warmupTime'] == result['measurementTime'] == '1000 ms'
            metric = result['primaryMetric']
            assert metric['scoreUnit'] == 'us/op' and list(map(len, metric['rawData'])) == [5]
            assert metric['score'] > 0
            resolved[key] = metric
        assert set(resolved) == expected, set(resolved) ^ expected
        arms[arm][repetition] = resolved
assert backends['copy'] == backends['view']
assert machines['copy'] == machines['view'] and resources['copy'] == resources['view']
assert hosts['copy'] == hosts['view'], hosts
assert definitions['copy']['profile'] == definitions['view']['profile']
assert definitions['copy']['nodeTags'] == definitions['view']['nodeTags']
rows = []
for repetition in range(2):
    for key in sorted(expected):
        before, after = arms['copy'][repetition][key], arms['view'][repetition][key]
        rows.append({
            'process_repetition': repetition, 'benchmark': key[0], 'parameters': json.dumps(dict(key[1]), sort_keys=True),
            'copy_us': before['score'], 'copy_engine_error': before['scoreError'],
            'view_us': after['score'], 'view_engine_error': after['scoreError'],
            'view_over_copy': after['score'] / before['score'], 'samples_per_arm': 5,
        })
csv_file('before-after.csv', rows)
csv_file('accuracy.csv', accuracies)
lines = [
    '# Backend-identified Native comparison', '',
    'Each process repetition retains five measurement samples after three warmups.',
    'Errors are the existing Native engine errors; process-to-process and host drift',
    'can exceed them. Ratios below one favor views. The order-32 control is unchanged',
    'numerical code. Inspect both repetitions and the raw samples before drawing a conclusion.', '',
]
for repetition in range(2):
    lines += [f'## Process repetition {repetition}', '',
              '| Method and fixture | Copy us/op | View us/op | View/copy |',
              '| --- | ---: | ---: | ---: |']
    for row in rows:
        if row['process_repetition'] == repetition:
            fixture = ', '.join(json.loads(row['parameters']).values())
            method = row['benchmark'].rsplit('.', 1)[-1]
            lines.append(f"| {method} {fixture} | {row['copy_us']:.3f} ± {row['copy_engine_error']:.3f} | {row['view_us']:.3f} ± {row['view_engine_error']:.3f} | {row['view_over_copy']:.3f} |")
    lines.append('')
(ROOT / 'tables.md').write_text('\n'.join(lines))
(ROOT / 'runtime.json').write_text(json.dumps({'backend': backends['copy'], 'hardware': machines['copy'], 'lab_host': hosts['copy'], 'resource_settings': resources['copy']}, indent=2) + '\n')
print(f'Validated both sources, backend/library identity, hardware/resource settings, numerical checks, and {len(rows)} matched Native comparisons.')
