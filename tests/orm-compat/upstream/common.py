"""Shared plumbing for the upstream ORM suite runners.

Upstream sources are fetched at the tags matching package.json into target/orm-compat-upstream
(gitignored). Nothing from them is copied into the repository.
"""
import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
HARNESS = HERE.parent
REPO = HARNESS.parent.parent
CACHE = pathlib.Path(os.environ.get('ORM_COMPAT_CACHE', REPO / 'target' / 'orm-compat-upstream'))
RUNS = CACHE / 'runs'
RESULTS = HARNESS / 'results' / 'upstream'

MOCHA_REPORTER = HERE / 'reporters' / 'mocha-jsonl.cjs'
VITEST_REPORTER = HERE / 'reporters' / 'vitest-jsonl.mjs'
JEST_REPORTER = HERE / 'reporters' / 'jest-jsonl.cjs'

PNPM = {v: CACHE / '.tools' / f'pnpm-{v}' / 'node_modules' / '.bin' / 'pnpm' for v in ('10.34.5', '10.6.3', '11.13.1')}

# Per-test timeout, identical for both targets so a hang costs the same everywhere.
TEST_TIMEOUT_MS = int(os.environ.get('ORM_COMPAT_TEST_TIMEOUT_MS', '20000'))
# Wall-clock cap per (suite, target) run, in seconds.
SUITE_CAP_S = int(os.environ.get('ORM_COMPAT_SUITE_CAP_S', str(3 * 3600)))

TARGETS = ('reference', 'surrealdb')


def log(*parts):
    print('[upstream]', *parts, flush=True)


def pinned_versions():
    return json.loads((HARNESS / 'package.json').read_text(encoding='utf-8'))['dependencies']


def run(cmd, cwd=None, env=None, check=True, timeout=None, out=None):
    log('$', ' '.join(str(c) for c in cmd), f'(cwd={cwd})' if cwd else '')
    stream = open(out, 'w', encoding='utf-8') if out else None
    try:
        r = subprocess.run([str(c) for c in cmd], cwd=cwd, env=env, stdout=stream, stderr=subprocess.STDOUT if stream else None, timeout=timeout)
    finally:
        if stream:
            stream.close()
    if check and r.returncode != 0:
        raise SystemExit(f'command failed ({r.returncode}): {cmd}')
    return r.returncode


def ensure_tools(*versions):
    for v in versions:
        if not PNPM[v].exists():
            run(['npm', 'install', '--silent', '--no-audit', '--no-fund', '--prefix', CACHE / '.tools' / f'pnpm-{v}', f'pnpm@{v}'])


def ensure_clone(name, url, tag, dirname=None):
    """Shallow-clone `url` at `tag` into the cache and return the checkout path."""
    dest = CACHE / (dirname or name)
    if not (dest / '.git').exists():
        CACHE.mkdir(parents=True, exist_ok=True)
        run(['git', '-c', 'advice.detachedHead=false', 'clone', '--quiet', '--depth', '1', '--branch', tag, url, dest])
    return dest


def resolved_commit(path):
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=path, text=True).strip()


def db_config(target, orm):
    """Connection parameters for the isolated upstream-suite database of `orm` on `target`."""
    if target == 'reference':
        return dict(host='127.0.0.1', port=15433, user='root', password='root', database=f'ref_upstream_{orm}', options=None)
    return dict(host='127.0.0.1', port=15432, user='root', password=None, database=f'orm/upstream_{orm}', options='-c dialect=pgsql')


def database_url(target, orm, database=None):
    c = db_config(target, orm)
    db = database or c['database']
    auth = f"{c['user']}:{c['password']}" if c['password'] else c['user']
    url = f"postgresql://{auth}@{c['host']}:{c['port']}/{db}"
    if c['options']:
        url += '?options=-c%20dialect%3Dpgsql'
    return url


def reset_reference_db(name):
    """Drop and recreate a reference database; the SurrealDB listener is restarted instead."""
    code = ("import pg from 'pg';const c=new pg.Client({host:'127.0.0.1',port:15433,user:'root',password:'root',database:'postgres'});"
            "await c.connect();await c.query('DROP DATABASE IF EXISTS %s WITH (FORCE)');await c.query('CREATE DATABASE %s');await c.end();" % (name, name))
    run(['node', '--input-type=module', '-e', code], cwd=HARNESS)


def run_streaming(cmd, cwd, env, jsonl, logfile, cap=None):
    """Run a test command in its own process group, killing the group at the wall-clock cap."""
    cap = cap or SUITE_CAP_S
    pathlib.Path(jsonl).unlink(missing_ok=True)
    log('$', ' '.join(str(c) for c in cmd), f'(cwd={cwd}, cap={cap}s)')
    with open(logfile, 'w', encoding='utf-8') as out:
        p = subprocess.Popen([str(c) for c in cmd], cwd=cwd, env=env, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        start = time.time()
        capped = False
        while p.poll() is None:
            time.sleep(2)
            if time.time() - start > cap:
                capped = True
                try:
                    os.killpg(p.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                time.sleep(5)
                if p.poll() is None:
                    try:
                        os.killpg(p.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                break
        p.wait()
    return dict(exit_code=p.returncode, capped=capped, wall_s=round(time.time() - start))


def read_jsonl(path):
    rows = {}
    p = pathlib.Path(path)
    if not p.exists():
        return []
    for line in p.read_text(encoding='utf-8', errors='replace').splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        rows[row['id']] = row
    return list(rows.values())


def write_result(orm, suite, target, meta, tests):
    RESULTS.mkdir(parents=True, exist_ok=True)
    doc = dict(orm=orm, suite=suite, target=target, **meta, tests=sorted(tests, key=lambda t: t['id']))
    path = RESULTS / f'{orm}-{suite}-{target}.json'
    path.write_text(json.dumps(doc, indent=1) + '\n')
    counts = {}
    for t in tests:
        counts[t['status']] = counts.get(t['status'], 0) + 1
    log(f'{orm}/{suite} on {target}:', counts, '->', path.relative_to(REPO))
    return path
