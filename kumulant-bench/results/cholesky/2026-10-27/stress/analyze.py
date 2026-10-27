"""Validate retained order-2048 stress/scaling captures and regenerate their tables (stdlib only)."""

import csv
import gzip
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROVENANCE = json.loads((ROOT / 'provenance.json').read_text())
FACTOR_CASES = ('precision:32', 'rbf:512', 'rbf:1024', 'rbf:2048', 'matern:2048')
SOLVE_SHAPES = ('32x1', '1024x256', '2048x1', '2048x256')
HISTORY_SIZES = ('512', '2048')
EXPECTED = (
    {(f'CholeskyFactorBenchmark.{method}', ('matrixCase', case))
     for method in ('factor', 'factorWithWorkspace') for case in FACTOR_CASES}
    | {(f'CholeskySolveBenchmark.{method}', ('shape', shape))
       for method in ('solveMatrix', 'solveMatrixInto') for shape in SOLVE_SHAPES}
    | {(f'CholeskyWorkloadBenchmark.{method}', ('historySize', size))
       for method in ('covarianceConstruction', 'factorSolveLogDeterminant', 'factorSolveLogDeterminantWithWorkspace')
       for size in HISTORY_SIZES}
)
EPSILON_TOLERANCE = 64 * 2.220446049250313e-16 * 2048 / 1e-6


def read(path):
    source = ROOT / path
    return gzip.decompress(source.read_bytes()).decode() if source.suffix == '.gz' else source.read_text()


def find(directory, name):
    matches = [path for path in (ROOT / directory).iterdir() if path.name in (name, name + '.gz')]
    assert len(matches) == 1, (directory, name)
    return matches[0].relative_to(ROOT).as_posix()


def csv_file(name, rows):
    with (ROOT / name).open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def owned_bytes(benchmark, value):
    """Caller-owned payload of the returned result; anything above it is temporary copies, packing or scratch."""
    method = benchmark.rsplit('.', 1)[-1]
    if method in ('factor', 'factorWithWorkspace'):
        order = int(value.split(':')[1])
        return 8 * order * order
    if method == 'solveMatrix':
        rows, columns = map(int, value.split('x'))
        return 8 * rows * columns
    order = int(value) if benchmark.startswith('CholeskyWorkload') else 0
    return {'solveMatrixInto': 0, 'covarianceConstruction': 8 * order * order,
            'factorSolveLogDeterminant': 8 * order * order + 8 * order,
            'factorSolveLogDeterminantWithWorkspace': 8 * order * order}[method]


def accuracy(text):
    """Re-checks the shared preflight's 54 factors, 12 updates, 108 extensions, 3 determinants and 3 retries."""
    counts = {'factor': 0, 'update': 0, 'extension': 0, 'diagonal': 0, 'retry': 0}
    maxima = {'factor': [0.0, 0.0], 'update': [0.0] * 4, 'extension': [0.0] * 4}
    for line in text.splitlines():
        cells = line.split(',')
        if cells[0] in ('precision', 'rbf', 'matern') and cells[1] != 'size':
            values, group = list(map(float, cells[3:5])), 'factor'
            assert values[0] < 1e-13 and values[1] < 1e-13
        elif cells[0] == 'update' and cells[1] != 'size':
            values, group = list(map(float, cells[3:7])), 'update'
            assert values[0] < 1e-13 and values[1] < 1e-13 and values[2] < 1e-10 and values[3] < 1e-9
        elif cells[0] == 'extension' and cells[1] != 'kernel':
            values, group = list(map(float, cells[5:10])), 'extension'
            assert values[0] < 1e-13 and values[1] < 1e-13 and values[2] < values[4] and values[3] < values[4]
            values = values[:4]
        elif cells[0] == 'diagonal' and cells[1] != 'size':
            assert float(cells[3]) <= 1e-10
            counts['diagonal'] += 1
            continue
        elif cells[0] == 'retry' and cells[1] != 'scenario':
            counts['retry'] += 1
            continue
        else:
            continue
        counts[group] += 1
        maxima[group] = [max(a, b) for a, b in zip(maxima[group], values)]
    assert counts == {'factor': 54, 'update': 12, 'extension': 108, 'diagonal': 3, 'retry': 3}, counts
    return maxima


def stress(text):
    backend = re.search(r'^Cholesky stress: seed=139, order=2048, backend=(.+)$', text, re.M).group(1)
    assert 'noiseVariance=1.0E-6, jitter=1.0E-10, matrixBytes=33554432' in text
    rows = []
    for line in text.splitlines():
        cells = line.split(',')
        if cells[0] == 'stress_factor' and cells[1] != 'kernel':
            values = list(map(float, cells[3:8]))
            assert all(value < 1e-13 for value in values)
            rows.append({'check': 'factor', 'kernel': cells[1], 'reconstruction': values[0], 'backward_error': values[1],
                         'into_backward_error': values[2], 'aliased_backward_error': values[3],
                         'workspace_factor_difference': values[4], 'relative_solution_error': '',
                         'absolute_logdet_error': '', 'forward_tolerance': ''})
        elif cells[0] == 'stress_sequence' and cells[1] != 'operation':
            values = list(map(float, cells[4:9]))
            assert values[0] < 1e-13 and values[1] < 1e-13 and values[2] < values[4] and values[3] < values[4]
            assert values[4] == EPSILON_TOLERANCE
            rows.append({'check': cells[1], 'kernel': cells[2], 'reconstruction': values[0], 'backward_error': values[1],
                         'into_backward_error': '', 'aliased_backward_error': '', 'workspace_factor_difference': '',
                         'relative_solution_error': values[2], 'absolute_logdet_error': values[3],
                         'forward_tolerance': values[4]})
    assert [(row['check'], row['kernel']) for row in rows] == [
        ('factor', 'rbf'), ('factor', 'matern'), ('extension', 'rbf'), ('extension', 'matern'),
        ('update', 'rbf'), ('update', 'matern')]
    return backend, rows


def results(path, native):
    resolved = {}
    for result in json.loads(read(path)):
        key = (result['benchmark'].removeprefix('com.eignex.kumulant.bench.'), next(iter(result['params'].items())))
        assert key not in resolved and len(result['params']) == 1
        assert result['mode'] == 'avgt' and result['warmupIterations'] == 3 and result['measurementIterations'] == 5
        assert result['warmupTime'] == result['measurementTime'] == '1000 ms'
        metric = result['primaryMetric']
        assert metric['scoreUnit'] == 'us/op' and metric['score'] > 0
        if native:
            assert result['compilationMode'] == 'Release' and list(map(len, metric['rawData'])) == [5]
            allocation = None
        else:
            assert result['forks'] == 2 and result['threads'] == 1 and list(map(len, metric['rawData'])) == [5, 5]
            assert {'-Xms512m', '-Xmx512m'} <= set(result['jvmArgs'])
            allocation = result['secondaryMetrics']['gc.alloc.rate.norm']['score']
            # JMH omits GC count/time when a benchmark collected no garbage.
            secondary = result['secondaryMetrics']
            metric = {**metric, 'gcCount': secondary.get('gc.count', {}).get('score', 0.0),
                      'gcTimeMs': secondary.get('gc.time', {}).get('score', 0.0)}
        resolved[key] = (metric, allocation)
    assert set(resolved) == EXPECTED, set(resolved) ^ EXPECTED
    return resolved


inventories, runtime, measurements, stresses, accuracies = 0, {}, [], [], []
for label, capture in PROVENANCE['captures'].items():
    lab = f'lab/{label}'
    definition = json.loads(read(f'{lab}/definition.json'))
    job = json.loads(read(find(lab, 'job.json')))
    assert job['id'] == capture['job'] and job['status'] == 'DONE'
    assert job['ref'] == definition['base']['ref'] == capture['ref'] and job['sha'] == capture['ref']
    assert read(f'{lab}/0.exit').strip() == '0'
    cases = json.loads(read(f'{lab}/cases.json'))
    assert len(cases) == 1 and cases[0]['status'] == 'DONE' and cases[0]['host']
    for entry in json.loads(read(f'{lab}/inventory.json')):
        data = (ROOT / entry['retained']).read_bytes()
        data = gzip.decompress(data) if entry['retained'].endswith('.gz') else data
        assert len(data) == entry['bytes'] and hashlib.sha256(data).hexdigest() == entry['sha256'], entry['source']
        inventories += 1
    log = read(f'{lab}/job.log')
    attempt = re.findall(r'committed:0:none:0:(\d+)', log)
    assert len(attempt) == 1, label
    resources = json.loads(read(f'{lab}/nomad-0-none-0-{attempt[0]}-resources.json'))
    assert resources['enforcement'] == 'cgroup-v2-hard' and resources['budget'] == {'cpuMillis': 1000, 'memoryMiB': 4608}
    assert resources['counters']['memory.events'].startswith('low 0\nhigh 0\nmax 0\noom 0\noom_kill 0')
    host = dict(line.split(':', 1) for line in read(f'{label}/host.txt').splitlines() if ':' in line)
    hardware = {key: host[key].strip() for key in ('Architecture', 'CPU(s)', 'Model name', 'Thread(s) per core', 'Core(s) per socket', 'Socket(s)') if key in host}
    flags = set(host.get('Flags', '').split())
    hardware['vector_flags'] = sorted(flags & {'avx2', 'avx512f', 'fma', 'asimd', 'sve', 'sve2'})
    entry = {'job': capture['job'], 'ref': capture['ref'], 'lab_host': cases[0]['host'], 'pool_tags': definition['nodeTags'],
             'hardware': hardware, 'resources': {key: resources[key] for key in ('enforcement', 'budget', 'cpuMax', 'memoryMax', 'javaVersion', 'os', 'arch')},
             'oom_events': 0, 'excluded_attempts': capture.get('excluded_attempts', [])}
    if capture['kind'] == 'native':
        quality = read(find(label, 'native-quality.txt'))
        identity = [line for line in quality.splitlines() if line.startswith('Cholesky ') and ':' in line.split(',')[0]][:5]
        assert len(identity) == 5 and identity[-1] == 'Cholesky threads: 1-thread-confirmed', identity
        backend, rows = stress(read(find(label, 'native-stress.txt')))
        assert identity[0] == f'Cholesky backend: {backend}'
        platforms = {}
        for repetition in range(2):
            assert all(read(find(label, f'native-{repetition}-benchmark.txt')).count(line) >= len(EXPECTED) for line in identity)
            platforms[f'native-{repetition}'] = (results(find(label, f'native-{repetition}-results.json'), True), quality, rows)
        entry['backends'] = {'native': identity}
    else:
        platforms, entry['backends'] = {}, {}
        for backend_name in ('scalar', 'simd'):
            quality = read(find(f'{label}/{backend_name}', 'quality.txt'))
            expected = 'KoblasEngine(simd(' if backend_name == 'simd' else 'KoblasEngine(scalar/'
            backend, rows = stress(read(find(f'{label}/{backend_name}', 'stress.txt')))
            assert backend.startswith(expected) and f'Cholesky accuracy: seed=139, backend={backend}' in quality
            platforms[f'jvm-{backend_name}'] = (results(find(f'{label}/{backend_name}', 'results.json'), False), quality, rows)
            entry['backends'][backend_name] = backend
            entry.setdefault('load_at_start', {})[backend_name] = read(f'{label}/{backend_name}/run.txt').split('load=')[1].strip()
    for platform, (resolved, quality, rows) in platforms.items():
        maxima = accuracy(quality)
        for group, values in maxima.items():
            accuracies.append({'capture': label, 'platform': platform, 'group': group,
                               **dict(zip(('max_reconstruction', 'max_backward_error', 'max_relative_solution_error', 'max_absolute_logdet_error'), values + ['', '']))})
        if not platform.endswith('-1'):
            stresses.extend({'capture': label, 'platform': platform.removesuffix('-0'), **row} for row in rows)
        for (benchmark, (name, value)), (metric, allocation) in sorted(resolved.items()):
            samples = [sample for fork in metric['rawData'] for sample in fork]
            owned = owned_bytes(benchmark, value)
            measurements.append({
                'capture': label, 'platform': platform, 'benchmark': benchmark, 'parameter': f'{name}={value}',
                'stress': '2048' in value, 'mean_us': metric['score'], 'engine_error_us': metric['scoreError'],
                'samples': len(samples), 'min_sample_us': min(samples), 'max_sample_us': max(samples),
                'bytes_per_op': '' if allocation is None else allocation, 'owned_result_bytes': owned,
                'other_bytes_per_op': '' if allocation is None else allocation - owned,
                'gc_count': metric.get('gcCount', ''), 'gc_time_ms': metric.get('gcTimeMs', ''),
            })
    runtime[label] = entry

csv_file('measurements.csv', measurements)
csv_file('stress.csv', stresses)
csv_file('accuracy.csv', accuracies)
(ROOT / 'runtime.json').write_text(json.dumps(runtime, indent=2) + '\n')

by_key = {(row['capture'], row['platform'], row['benchmark'], row['parameter']): row for row in measurements}


def cell(row):
    return f"{row['mean_us'] / 1000:.3f} ± {row['engine_error_us'] / 1000:.3f}"


lines = ['# Order-2048 stress and scaling tables', '',
         'Generated by `analyze.py`. Times are milliseconds: engine mean ± engine error (JMH 99.9% interval over',
         'two forks, or the Native engine over one process). Each capture ran on its own host; rows from different',
         'captures are a descriptive inventory, not a controlled comparison. MB is decimal megabytes per operation',
         'from the JMH GC profiler, split into the caller-owned result and everything else. Native has no',
         'allocation profiler.', '']
for label in PROVENANCE['captures']:
    entry = runtime[label]
    platforms = sorted({row['platform'] for row in measurements if row['capture'] == label})
    lines += [f"## {label}: {entry['hardware']['Model name']}", '',
              f"Job {entry['job']}, Lab host `{entry['lab_host']}`, {entry['hardware']['Architecture']} "
              f"{entry['hardware']['CPU(s)']} CPUs, flags {', '.join(entry['hardware']['vector_flags'])}; backends: "
              + '; '.join(f'`{value if isinstance(value, str) else value[0].split(": ", 1)[1]}`' for value in entry['backends'].values()) + '.', '']
    for stress_only, title in ((True, 'Order-2048 stress'), (False, 'Small and mid-size controls')):
        lines += [f'### {title}', '', '| Method | Fixture | ' + ' | '.join(f'{platform} ms' for platform in platforms)
                  + (' | ' + ' | '.join(f'{platform} owned/other MB' for platform in platforms if platform.startswith('jvm')) if platforms[0].startswith('jvm') else '') + ' |',
                  '| --- | --- |' + ' ---: |' * (len(platforms) + sum(platform.startswith('jvm') for platform in platforms))]
        for benchmark, parameter in sorted({(row['benchmark'], row['parameter']) for row in measurements
                                            if row['capture'] == label and row['stress'] == stress_only}):
            rows = [by_key[(label, platform, benchmark, parameter)] for platform in platforms]
            memory = [f"{row['owned_result_bytes'] / 1e6:.3f} / {row['other_bytes_per_op'] / 1e6:.3f}" for row in rows if row['bytes_per_op'] != '']
            lines.append(f"| {benchmark.rsplit('.', 1)[-1]} | {parameter.split('=')[1]} | " + ' | '.join(cell(row) for row in rows)
                         + (' | ' + ' | '.join(memory) if memory else '') + ' |')
        lines.append('')
lines += ['## Order-2048 numerical stress', '',
          '| Capture | Platform | Check | Kernel | Reconstruction | Backward | Relative solution | Log-det | Tolerance |',
          '| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |']
for row in stresses:
    lines.append(f"| {row['capture']} | {row['platform']} | {row['check']} | {row['kernel']} | {row['reconstruction']:.3g} | "
                 f"{row['backward_error']:.3g} | {row['relative_solution_error'] and format(row['relative_solution_error'], '.3g')} | "
                 f"{row['absolute_logdet_error'] and format(row['absolute_logdet_error'], '.3g')} | "
                 f"{row['forward_tolerance'] and format(row['forward_tolerance'], '.3g')} |")
(ROOT / 'tables.md').write_text('\n'.join(lines) + '\n')
print(f'Validated {len(runtime)} captures, {inventories} retained files, {len(stresses)} stress checks, '
      f'{len(accuracies)} accuracy groups and {len(measurements)} measurements.')
