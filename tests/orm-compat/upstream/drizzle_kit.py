"""drizzle-kit: the pg tests that execute SQL, run with vitest.

Upstream creates `new PGlite()` in-process. A vitest alias swaps the module for shims/pglite.mjs,
which gives each instance a fresh database on the target server. Tests that only diff schemas
(pg-tables, pg-columns, ...) never execute SQL and are not run: they would pass identically anywhere.
"""
import os

import drizzle

from common import *

SUITE = 'kit-pg'
FILES = ['tests/push/pg.test.ts', 'tests/introspect/pg.test.ts']


def setup():
    return (drizzle.setup_monorepo(),)


def run_target(target, src):
    kit = src / 'drizzle-kit'
    cfg = db_config(target, 'drizzle_kit')
    config = kit / 'vitest.orm-compat.config.ts'
    config.write_text(f"""import tsconfigPaths from 'vite-tsconfig-paths';
import {{ defineConfig }} from 'vitest/config';
export default defineConfig({{
	resolve: {{ alias: {{ '@electric-sql/pglite': '{HERE / 'shims' / 'pglite.mjs'}' }} }},
	server: {{ fs: {{ strict: false }} }},
	test: {{ include: {FILES!r}, testTimeout: {TEST_TIMEOUT_MS}, hookTimeout: {TEST_TIMEOUT_MS}, isolate: true,
		poolOptions: {{ threads: {{ singleThread: true }} }}, maxWorkers: 1, fileParallelism: false }},
	plugins: [tsconfigPaths()],
}});
""")
    out = RUNS / 'drizzle-kit'
    out.mkdir(parents=True, exist_ok=True)
    jsonl = out / f'{SUITE}-{target}.jsonl'
    env = dict(os.environ, ORM_COMPAT_JSONL=str(jsonl), ORM_COMPAT_ROOT=str(kit), ORM_COMPAT_TARGET=target,
               TEST_CONFIG_PATH_PREFIX='./tests/cli/', DOCKER_HOST='unix:///nonexistent-orm-compat')
    meta = run_streaming([kit / 'node_modules' / '.bin' / 'vitest', 'run', '--config', config, '--reporter', VITEST_REPORTER],
                         cwd=kit, env=env, jsonl=jsonl, logfile=out / f'{SUITE}-{target}.log')
    meta.update(version=pinned_versions()['drizzle-kit'], commit=resolved_commit(src), test_timeout_ms=TEST_TIMEOUT_MS, files=FILES,
                connection=dict(host=cfg['host'], port=cfg['port'], note='fresh database per PGlite instance'))
    return SUITE, meta, read_jsonl(jsonl)
