"""drizzle-orm: the pg integration tests that take a connection string, run with vitest."""
import os
import pathlib

from common import *

SUITE = 'pg'
# Files that read PG_CONNECTION_STRING. Docker-only (vectors, postgis) and in-process PGlite files
# (seeder, rls) cannot be pointed at a server and are not run.
FILES = [
    'tests/pg/node-postgres.test.ts',
    'tests/pg/postgres-js.test.ts',
    'tests/pg/pg-custom.test.ts',
    'tests/pg/pg-proxy.test.ts',
    'tests/relational/pg.test.ts',
    'tests/relational/pg.postgresjs.test.ts',
    'tests/relational/issues-schemas/wrong-mapping/pg.test.ts',
    'tests/relational/issues-schemas/duplicates/pg/pg.duplicates.test.ts',
]


def setup_monorepo():
    ensure_tools('10.6.3')
    version = pinned_versions()['drizzle-orm']
    src = ensure_clone('drizzle', 'https://github.com/drizzle-team/drizzle-orm', version)
    pnpm = PNPM['10.6.3']
    env = dict(os.environ, PATH=f'{pnpm.parent}:{os.environ["PATH"]}')
    if not (src / 'integration-tests' / 'node_modules').exists():
        # Lifecycle scripts are skipped: better-sqlite3 and similar native builds are not used by the pg suites.
        run([pnpm, 'install', '--frozen-lockfile', '--ignore-scripts', '--store-dir', CACHE / '.pnpm-store',
             '--filter', 'drizzle-orm...', '--filter', 'integration-tests...', '--filter', 'drizzle-kit...'], cwd=src, env=env)
    if not (src / 'drizzle-orm' / 'dist').exists():
        run([pnpm, 'build'], cwd=src / 'drizzle-orm', env=env)
    return src


def setup():
    return (setup_monorepo(),)


def run_target(target, src):
    tests = src / 'integration-tests'
    cfg = db_config(target, 'drizzle')
    if target == 'reference':
        reset_reference_db(cfg['database'])
    out = RUNS / 'drizzle'
    out.mkdir(parents=True, exist_ok=True)
    jsonl = out / f'{SUITE}-{target}.jsonl'
    env = dict(os.environ, ORM_COMPAT_JSONL=str(jsonl), ORM_COMPAT_ROOT=str(tests),
               PG_CONNECTION_STRING=database_url(target, 'drizzle'), DOCKER_HOST='unix:///nonexistent-orm-compat')
    meta = run_streaming([tests / 'node_modules' / '.bin' / 'vitest', 'run', *FILES, '--reporter', VITEST_REPORTER,
                          '--testTimeout', TEST_TIMEOUT_MS, '--hookTimeout', TEST_TIMEOUT_MS],
                         cwd=tests, env=env, jsonl=jsonl, logfile=out / f'{SUITE}-{target}.log')
    meta.update(version=pinned_versions()['drizzle-orm'], commit=resolved_commit(src), test_timeout_ms=TEST_TIMEOUT_MS, files=FILES,
                connection=dict(host=cfg['host'], port=cfg['port'], database=cfg['database']))
    return SUITE, meta, read_jsonl(jsonl)
