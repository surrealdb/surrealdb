"""TypeORM: the postgres functional tests, run with mocha against each target."""
import json
import os
import pathlib

from common import *

SUITE = 'functional'


def setup():
    ensure_tools('10.34.5')
    version = pinned_versions()['typeorm']
    src = ensure_clone('typeorm', 'https://github.com/typeorm/typeorm', version)
    pkg = src / 'packages' / 'typeorm'
    if not (pkg / 'node_modules').exists():
        run([PNPM['10.34.5'], 'install', '--frozen-lockfile', '--store-dir', CACHE / '.pnpm-store', '--filter', 'typeorm...'], cwd=src)
    if not (pkg / 'build' / 'compiled' / 'test').exists():
        run([pkg / 'node_modules' / '.bin' / 'tsc'], cwd=pkg)
    return src, pkg


def run_target(target, src, pkg):
    cfg = db_config(target, 'typeorm')
    if target == 'reference':
        reset_reference_db(cfg['database'])
    entry = dict(skip=False, name='postgres', type='postgres', host=cfg['host'], port=cfg['port'], username=cfg['user'],
                 database=cfg['database'], logging=False)
    if cfg['password']:
        entry['password'] = cfg['password']
    if cfg['options']:
        entry['extra'] = dict(options=cfg['options'])
    (pkg / 'ormconfig.json').write_text(json.dumps([entry], indent=2))
    mocharc = pkg / '.orm-compat-mocharc.json'
    mocharc.write_text(json.dumps({
        'check-leaks': True, 'exit': True, 'recursive': True,
        'file': ['./build/compiled/test/utils/test-setup.js'],
        'globals': ['allowedDriverRequire'],
        'spec': ['./build/compiled/test/functional/**/*.test.js'],
        'timeout': TEST_TIMEOUT_MS,
    }))
    out = RUNS / 'typeorm'
    out.mkdir(parents=True, exist_ok=True)
    jsonl = out / f'{SUITE}-{target}.jsonl'
    env = dict(os.environ, ORM_COMPAT_JSONL=str(jsonl), ORM_COMPAT_ROOT=str(pkg))
    meta = run_streaming([pkg / 'node_modules' / '.bin' / 'mocha', '--config', mocharc, '--reporter', MOCHA_REPORTER],
                         cwd=pkg, env=env, jsonl=jsonl, logfile=out / f'{SUITE}-{target}.log')
    meta.update(version=pinned_versions()['typeorm'], commit=resolved_commit(src), test_timeout_ms=TEST_TIMEOUT_MS,
                connection=dict(host=cfg['host'], port=cfg['port'], database=cfg['database']))
    return SUITE, meta, read_jsonl(jsonl)
