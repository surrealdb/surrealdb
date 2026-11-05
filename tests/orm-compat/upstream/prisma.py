"""Prisma: client functional tests (driver adapter js_pg) and the migrate package's postgres tests, run with jest.

Both suites need the upstream monorepo installed and built. Lifecycle scripts are skipped at install
(native better-sqlite3 does not build on every Node); the schema engine binary that the harness's own
`npm ci` already downloaded is handed to the tests through PRISMA_SCHEMA_ENGINE_BINARY.
Results come from jest-junit (the reporter upstream CI uses), one file per shard so a capped shard
loses only itself.
"""
import glob
import os
import re
import xml.etree.ElementTree as ET

from common import *

PNPM_VERSION = '11.13.1'
SHARDS = int(os.environ.get('ORM_COMPAT_PRISMA_SHARDS', '10'))
FILTERS = ['--filter', '@prisma/client...', '--filter', '@prisma/migrate...', '--filter', 'prisma...']


def engine_binary():
    for p in glob.glob(str(HARNESS / 'node_modules' / '@prisma' / 'engines' / 'schema-engine-*')):
        return p
    raise SystemExit('schema-engine binary not found; run `npm ci` in tests/orm-compat')


def setup():
    ensure_tools(PNPM_VERSION)
    version = pinned_versions()['prisma']
    src = ensure_clone('prisma', 'https://github.com/prisma/prisma', version)
    pnpm = PNPM[PNPM_VERSION]
    env = dict(os.environ, PATH=f'{pnpm.parent}:{os.environ["PATH"]}')
    if not (src / 'node_modules').exists():
        run([pnpm, 'install', '--frozen-lockfile', '--ignore-scripts', '--store-dir', CACHE / '.pnpm-store', *FILTERS], cwd=src, env=env)
    if not (src / 'packages' / 'client' / 'runtime').exists():
        run([pnpm, 'exec', 'turbo', 'run', 'build', '--filter=@prisma/client...', '--filter=@prisma/migrate...', '--filter=prisma...'], cwd=src, env=env)
    return (src,)


def sqlstate(text):
    m = re.search(r"\bcode: '([0-9A-Z]{5})'", text) or re.search(r'Code: `([0-9A-Z]{5})`', text) or re.search(r'SQLSTATE[ =:]*([0-9A-Z]{5})', text)
    return m.group(1) if m else None


GUARD = 'PRISMA_USER_CONSENT_FOR_DANGEROUS_AI_ACTION'


def parse_junit(path, keep=None):
    """Return (tests, guard_blocked). Prisma refuses destructive commands such as `db push --force-reset`
    when it detects an AI agent unless the user's own consent text is exported in the variable named by
    GUARD. Tests stopped by that guard did not exercise the database and are counted separately, never
    as passes or failures."""
    tests, blocked = [], 0
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, FileNotFoundError):
        return tests, blocked
    for case in root.iter('testcase'):
        tid = f"{case.get('file') or case.get('classname')}::{case.get('classname')} {case.get('name')}"
        if keep and not keep(tid):
            continue
        failure = case.find('failure')
        if failure is None:
            failure = case.find('error')
        if failure is not None:
            text = re.sub(r'\x1b\[[0-9;]*m', '', (failure.get('message') or '') + '\n' + (failure.text or ''))
            if GUARD in text:
                blocked += 1
                continue
            tests.append(dict(id=tid, status='fail', message=text.strip()[:2000], code=sqlstate(text)))
        elif case.find('skipped') is not None:
            tests.append(dict(id=tid, status='skip'))
        else:
            tests.append(dict(id=tid, status='pass'))
    return tests, blocked


def functional_uri(target):
    if target == 'reference':
        return 'postgres://root:root@127.0.0.1:15433/PRISMA_DB_NAME'
    return 'postgres://root@127.0.0.1:15432/orm/PRISMA_DB_NAME?options=-c%20dialect%3Dpgsql'


def run_shards(src, target, suite, pkg, cmd_for_shard, env, junit_name, keep=None):
    out = RUNS / 'prisma'
    out.mkdir(parents=True, exist_ok=True)
    tests, meta = [], dict(exit_code=0, capped=False, wall_s=0, shards=[], guard_blocked=0)
    for shard in range(1, SHARDS + 1):
        junit = out / f'{suite}-{target}-{shard}.xml'
        junit.unlink(missing_ok=True)
        e = dict(env, JEST_JUNIT_OUTPUT_DIR=str(out), JEST_JUNIT_OUTPUT_NAME=junit.name, JEST_JUNIT_DISABLE='false')
        m = run_streaming(cmd_for_shard(shard), cwd=pkg, env=e, jsonl=out / 'unused.jsonl', logfile=out / f'{suite}-{target}-{shard}.log',
                          cap=SUITE_CAP_S // SHARDS)
        meta['shards'].append(m)
        meta['wall_s'] += m['wall_s']
        meta['capped'] = meta['capped'] or m['capped']
        got, blocked = parse_junit(junit, keep)
        tests += got
        meta['guard_blocked'] += blocked
        log_text = (out / f'{suite}-{target}-{shard}.log').read_text(encoding='utf-8', errors='replace') if (out / f'{suite}-{target}-{shard}.log').exists() else ''
        if GUARD in log_text and not got:
            meta['guard_blocked'] += 1
    meta['exit_code'] = max((s['exit_code'] or 0) for s in meta['shards'])
    return meta, tests


def dotenv(path):
    values = {}
    for line in path.read_text(encoding='utf-8').splitlines():
        line = line.split(' #')[0].strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            values[k.strip()] = v.strip().strip('"')
    return values


def base_env(src):
    # The suites resolve connection strings for every flavor they plan, selected or not, so the
    # upstream .db.env supplies all variables and the postgres ones are overridden by the caller.
    env = dict(os.environ)
    env.update(dotenv(src / '.db.env'))
    env.update(PRISMA_SCHEMA_ENGINE_BINARY=engine_binary(), PRISMA_HIDE_UPDATE_MESSAGE='true',
               DOCKER_HOST='unix:///nonexistent-orm-compat', CI='')
    return env


def run_target(target, src):
    version = pinned_versions()['prisma']
    results = []
    pkg = src / 'packages' / 'client'
    env = dict(base_env(src), TEST_FUNCTIONAL_POSTGRES_URI=functional_uri(target), TEST_FUNCTIONAL_POSTGRES_16_URI=functional_uri(target))
    if target == 'reference':
        reset_reference_db('ref_upstream_prisma')
    tsx = src / 'node_modules' / '.bin' / 'tsx'
    meta, tests = run_shards(src, target, 'client', pkg, lambda s: [
        tsx, 'helpers/functional-test/run-tests.ts', '--adapter', 'js_pg', '--generator-type', 'prisma-client-ts',
        '--client-runtime', 'client', '--no-types', '--runInBand', '--shard', f'{s}/{SHARDS}'], env, None)
    meta.update(version=version, commit=resolved_commit(src), test_timeout_ms=30000, adapter='js_pg',
                note='tests/functional with --adapter js_pg, --no-types (type tests are DB-independent)')
    results.append(('client-functional', meta, tests))

    pkg = src / 'packages' / 'migrate'
    if target == 'reference':
        base = 'postgres://root:root@127.0.0.1:15433/'
        names = ('ref_prisma_tests', 'ref_prisma_tests_migrate', 'ref_prisma_tests_migrate_shadowdb')
        for n in names:
            reset_reference_db(n)
        uris = dict(TEST_POSTGRES_URI=base + names[0], TEST_POSTGRES_URI_MIGRATE=base + names[1], TEST_POSTGRES_SHADOWDB_URI_MIGRATE=base + names[2])
    else:
        base, opt = 'postgres://root@127.0.0.1:15432/orm/', '?options=-c%20dialect%3Dpgsql'
        uris = dict(TEST_POSTGRES_URI=base + 'tests' + opt, TEST_POSTGRES_URI_MIGRATE=base + 'tests-migrate' + opt,
                    TEST_POSTGRES_SHADOWDB_URI_MIGRATE=base + 'tests-migrate-shadowdb' + opt)
    env = dict(base_env(src), **uris)
    jest = pkg / 'node_modules' / '.bin' / 'jest'
    meta, tests = run_shards(src, target, 'migrate', pkg, lambda s: [
        jest, '--runInBand', '--shard', f'{s}/{SHARDS}', 'DbPush', 'DbPull/postgresql', 'DbExecute', 'MigrateDeploy', 'MigrateDev', 'MigrateDiff', 'rpc'],
        env, None, keep=lambda tid: re.search(r'postgres', tid, re.I) is not None)
    meta.update(version=version, commit=resolved_commit(src), test_timeout_ms=10000,
                note='postgres-named tests from the migrate package files that read TEST_POSTGRES_*')
    results.append(('migrate-postgres', meta, tests))
    return results
