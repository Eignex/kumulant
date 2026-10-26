#!/usr/bin/env python3
"""Regenerate the embedded Joe-Kuo D(6) prefix from the original direction-number file."""
import argparse
import hashlib
from pathlib import Path

SOURCE_SHA256 = '68eedd2a4e3b659b9695e7aff0f8ac68718bcf620730fc3d3a8c65df2a067441'
SOURCE_URL = 'https://web.maths.unsw.edu.au/~fkuo/sobol/new-joe-kuo-6.21201'


def main():
    parser = argparse.ArgumentParser(description=f'Download the source from {SOURCE_URL} first.')
    parser.add_argument('source', type=Path)
    args = parser.parse_args()
    payload = args.source.read_bytes()
    if hashlib.sha256(payload).hexdigest() != SOURCE_SHA256:
        parser.error('Source SHA256 does not match the Joe-Kuo D(6) table.')
    records = [line.split() for line in payload.decode().splitlines()[1:1024]]
    assert [int(row[0]) for row in records] == list(range(2, 1025))
    for row in records:
        assert len(row) == int(row[1]) + 3
    destination = Path('kumulant/src/commonMain/kotlin/com/eignex/kumulant/math/SobolDirections.kt')
    prefix = destination.read_text().split('private val sobolParameterRows = arrayOf(')[0]
    literals = ''.join('    "' + ','.join(row[1:]) + '",\n' for row in records)
    destination.write_text(prefix + 'private val sobolParameterRows = arrayOf(\n' + literals + ')\n')


if __name__ == '__main__':
    main()
