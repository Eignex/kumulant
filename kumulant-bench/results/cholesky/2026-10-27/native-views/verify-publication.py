"""Verify recorded immutable Maven objects and published source checksums without building code."""

import hashlib
import io
import json
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent / 'publication'
publication = json.loads((ROOT / 'artifacts.json').read_text())
proof = json.loads((ROOT / 'source-proof.json').read_text())
assert proof['merge'] == publication['runtime_merge']
checked = 0
for artifact in publication['artifacts']:
    for item in artifact['files']:
        with urllib.request.urlopen(item['url'], timeout=30) as response:
            data = response.read()
        assert len(data) == item['bytes'] and hashlib.sha256(data).hexdigest() == item['sha256'], item['url']
        if item['name'].endswith('-sources.jar'):
            with zipfile.ZipFile(io.BytesIO(data)) as sources:
                for source in proof['sources']:
                    if source['artifact'] == artifact['artifact']:
                        assert hashlib.sha256(sources.read(source['source'])).hexdigest() == source['sha256']
                        checked += 1
assert checked == len(proof['sources'])
print(f"Verified {len(publication['artifacts'])} published artifacts and {checked} source files.")
