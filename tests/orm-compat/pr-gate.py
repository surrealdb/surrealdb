#!/usr/bin/env python3
"""Pass/fail gate for the pull-request subset of the ORM harness.

Compares the SurrealDB results of the runtime checks (results.json) and the schema tools
(cli-results.json) with the known failures listed in expected-failures.json. The run fails when a
case outside that list does not pass, and when a listed case passes, so the list shrinks as gaps
close and cannot hide a regression.
"""
import json
import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent
expected = set(json.loads((root / 'expected-failures.json').read_text())['expected_failures'])

observed = {}
for r in json.loads((root / 'results.json').read_text())['results']:
    observed[f"runtime/{r['orm']}/{r['name']}"] = r['status']
for r in json.loads((root / 'cli-results.json').read_text()):
    if r['target'] == 'surrealdb':
        observed[f"schema/{r['orm']}/{r['name']}"] = r['status']

unexpected_failures = sorted(k for k, v in observed.items() if v != 'pass' and k not in expected)
unexpected_passes = sorted(k for k in expected if observed.get(k) == 'pass')
missing = sorted(k for k in expected if k not in observed)

passed = sum(1 for v in observed.values() if v == 'pass')
print(f'{len(observed)} cases: {passed} pass, {len(observed) - passed} not passing, {len(expected)} expected failures')
for k in unexpected_failures:
    print(f'UNEXPECTED FAILURE {k} ({observed[k]})')
for k in unexpected_passes:
    print(f'UNEXPECTED PASS {k}: remove it from expected-failures.json')
for k in missing:
    print(f'NOT RUN {k}: listed in expected-failures.json but absent from the results')
sys.exit(1 if unexpected_failures or unexpected_passes or missing else 0)
