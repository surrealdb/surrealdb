# PostgreSQL ORM compatibility harness

These targeted checks exercise real Prisma, Drizzle and TypeORM clients through the PostgreSQL
wire listener. They cover scalar CRUD, projection ordering, filters and pagination, transactions,
relations, grouping, upserts, complex values, generated integer IDs and duplicate primary keys.
Schema-tool checks exercise introspection, generated migration application, and repeat application.
They are a compatibility probe, not a performance benchmark. The ORMs' own upstream suites run
separately, see "Upstream suites" below.

## One command

```sh
cargo make orm-compat harness             # or: tests/orm-compat/run.sh harness
cargo make orm-compat harness --record    # also refresh results/ (needs every reference check to pass)
tests/orm-compat/run.sh pr                # pull-request subset: SurrealDB only, gated by expected-failures.json
cargo make orm-compat upstream            # all upstream suites; or name one: typeorm drizzle drizzle-kit prisma
```

The script builds the example listener (release profile; `ORM_COMPAT_PROFILE=dev` for a debug
build), runs `npm ci` when `node_modules` is missing, and refuses to start if port 15432 or 15433 is
already taken, because a second listener on a taken port fails silently. It then starts the
SurrealDB listener and the PostgreSQL 18.4 reference, runs the runtime checks on SurrealDB, the same
checks with `REFERENCE=1`, and the schema suites, and stops both servers on exit, including on
Ctrl-C. Server logs go to `target/orm-compat-logs`. `run.sh services` only starts both servers.
The `ORM compatibility` workflow runs the harness and the upstream suites nightly on `main` (and on manual dispatch) and uploads the results; it measures and is not a required check.

## Pull-request subset

The `ORM compatibility (Prisma, Drizzle, TypeORM)` job in `ci.yml` runs `run.sh pr` (through `cargo make ci-orm-compat`) and is part of the required checks. It serves the checks from the `devopt` `surreal` binary that the `build` job produces (`ORM_COMPAT_SERVER`; run locally without it, the script builds the example listener in the dev profile), runs the runtime checks and the Prisma and Drizzle schema tools against SurrealDB only, and skips the PostgreSQL reference and the upstream suites, which stay nightly. A run takes about a minute after the build.

`pr-gate.py` compares every case with `expected-failures.json`, whose entries are `runtime/<orm>/<name>` or `schema/<orm>/<name>`. The run fails on a case that does not pass and is not listed, and on a listed case that passes, so the list shrinks as gaps close. The list is empty when every case passes. Add an entry only for a gap that is understood and tracked; never to silence a regression.

The job runs on pull requests that touch the SQL front ends, the wire listeners, the engine crates they execute on, this directory, the lockfile or the CI wiring (the `orm` output of the `changes` job), and on every push. `cli-suites.py` bounds each schema-tool command at 300 seconds and reports each command's duration.

The steps below are what the script does, for running them by hand.

The runtime fixtures use explicit integer IDs and pre-created tables. Generated IDs and schema
management are tested separately so failures there do not mask application-query coverage. A
complex-value check combines JSON, text arrays, decimals and timestamps: its first failure does
not establish which later types work. TypeORM checks that cannot run after initialization fails
are recorded as blocked. A failure is not counted as a pass merely because it fails safely.

## Run the SurrealDB checks

Start the in-memory listener from the repository root:

```sh
cargo run --locked -p surrealdb-pgwire --example orm_compat_server
```

In another terminal, from this directory:

```sh
npm ci
npm run generate
npm test
```

The listener uses the server runtime's worker-stack sizes and binds only `127.0.0.1:15432`. Clients use database names `orm/prisma`, `orm/drizzle`
and `orm/typeorm` and startup option `-c dialect=pgsql`. The suite creates and replaces test rows
in those databases. `PGHOST` and `PGPORT` select the runtime target. There is no authentication or
TLS test in this harness. Default credentials and test databases belong only to isolated listeners.

The process records every scenario and generated SQL in `results.json`, attempts all independent
scenarios, and exits nonzero when a scenario fails or is blocked. It exits after flushing the report so
connections left open by failed ORM initialization cannot hold the runner open.

## Validate against PostgreSQL

The optional `embedded-postgres` test dependency supplies a separate PostgreSQL reference server.
It stores only local test data in `.postgres-reference` and binds the reference port `15433`:

```sh
npm run reference
```

In another terminal:

```sh
REFERENCE=1 PGPORT=15433 RESULT_PATH=reference-results.json npm test
```

The reference uses user/password `root`/`root` in its isolated cluster. Its runtime fixtures live in
the `postgres` database. Do not point either suite at a database containing other work.

## Schema tools

With both isolated listeners running:

```sh
npm run schema-suites
```

This invokes the pinned Prisma CLI and Drizzle Kit against both backends. It creates the
`ref_prisma_migrate` and `ref_drizzle_migrate` databases on the reference cluster, dropping prior
copies of those two test databases, and uses `orm/prisma_migrate` and `orm/drizzle_migrate` on
SurrealDB. It verifies that successful migration commands actually created the expected tables.
Introspection must find the fixture models. Commands have bounded timeouts; output and driver
SQL errors go to `cli-logs`, and results go to `cli-results.json`.

Connection URLs for node-postgres clients use a literal namespace/database slash, such as
`postgresql://root@127.0.0.1:15432/orm/drizzle?options=-c%20dialect%3Dpgsql`.
The pinned node-postgres connection parser preserves a percent-encoded slash literally, which
would select a different database name. Prisma's schema engine keeps only the first path segment,
so its URLs name the namespace as a startup option and the database alone in the path:
`postgresql://root@127.0.0.1:15432/prisma_migrate?options=-c%20namespace%3Dorm%20-c%20dialect%3Dpgsql`. The Prisma runtime uses `@prisma/adapter-pg`; Drizzle uses
its node-postgres adapter. The suites do not rewrite ORM SQL or bypass TypeORM initialization; driver wrappers only record
SQL and errors.

## Recorded evidence

`results/` contains immutable runtime results, schema-tool diagnostics, reference environment
and an aggregate summary. The summary identifies the source tree measured. These are observed
results for the pinned versions and scenarios, not a general compatibility certification.
After running both runtime suites and the schema tools, run `python3 record-results.py` while the
reference listener is running. It requires passing reference checks, equal scenario sets and
unique case identities before recording evidence. `SOURCE_COMMIT` and `SOURCE_TREE` can identify the product revision separately from a later
harness-only commit. Regenerate evidence only after checking all
reference scenarios pass.

## Upstream suites

`./run.sh upstream [typeorm|drizzle|drizzle-kit|prisma ...]` runs each ORM's own tests against the
SurrealDB listener and against the PostgreSQL 18.4 reference, then classifies every test by joining the two runs:

| bucket | meaning |
|---|---|
| `pass_both` | passes on both |
| `surrealdb_only` | fails on SurrealDB, passes on the reference: a real gap |
| `fail_both` | fails on both: environment or version noise |
| `reference_only` | fails only on the reference |
| `not_run_surrealdb` | passes on the reference but never ran on SurrealDB (a failed hook skipped it) |
| `skipped` | skipped on either side |

A listener restart between ORMs keeps runs independent. The SurrealDB pass rate reported is
`pass_both / (tests that pass on the reference)`. Failure causes are grouped by SQLSTATE where the
error carries one, otherwise by the first message line with numbers and quoted names removed.

Upstream sources are cloned at the tags matching `package.json` into `target/orm-compat-upstream`
(gitignored, nothing is copied into the repository) together with their own pnpm install, store and
build output. Delete that directory to reclaim the disk; the next run fetches it again.
`ORM_COMPAT_CACHE` relocates it. Tunables: `ORM_COMPAT_TEST_TIMEOUT_MS` (per test, default 20000,
the same for both targets) and `ORM_COMPAT_SUITE_CAP_S` (wall clock per suite and target, default 3 hours).

Scope per ORM, and what is deliberately not run:

- **TypeORM 1.1.2**: `test/functional` under mocha with only a `postgres` entry in `ormconfig.json`.
- **drizzle-orm 0.45.4**: the pg integration files that read `PG_CONNECTION_STRING` (node-postgres, postgres-js, pg-custom, pg-proxy and the relational pg files). `DOCKER_HOST` points nowhere so no file can silently start its own PostgreSQL. Vector/PostGIS (Docker only), the seeder and RLS files (in-process PGlite) cannot be pointed at a server.
- **drizzle-kit 0.31.11**: `push/pg` and `introspect/pg`, the pg tests that execute SQL. Upstream runs them on in-process PGlite, so a vitest alias substitutes `upstream/shims/pglite.mjs`, which gives every `new PGlite()` a fresh database on the target server. The schema-diff-only pg tests never execute SQL and would pass anywhere, so they are not run. Roles are cluster-global on the reference, so the shim resets them per instance.
- **Prisma 7.10.1**: `packages/client` functional tests with the `js_pg` adapter (code tests only; the type tests do not touch a database) and the postgres tests of `packages/migrate`, in 10 jest shards each so one capped shard loses only itself. Install skips lifecycle scripts; the schema engine downloaded by this harness's own `npm ci` is passed in `PRISMA_SCHEMA_ENGINE_BINARY`. Prisma refuses `db push --force-reset` when it detects an AI agent unless the user's own consent text is exported in `PRISMA_USER_CONSENT_FOR_DANGEROUS_AI_ACTION`. The runner never sets that variable. Blocked tests are counted as `guard_blocked`, excluded from the classification, and a suite that is mostly blocked is reported as not measured. Run the script from your own terminal, with that variable exported if you accept the reset of the isolated test databases, to measure the client suite.

Outputs in `results/upstream/`: one `<orm>-<suite>-<target>.json` per run (every test with status, message and SQLSTATE), `classification.json` (per-test bucket for every test except those passing on both or skipped), `summary.json` and `summary.md` (counts and top causes). The runners and aggregator need only the Python standard library.
