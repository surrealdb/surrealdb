# PostgreSQL ORM compatibility harness

These targeted checks exercise real Prisma, Drizzle and TypeORM clients through the PostgreSQL
wire listener. They cover scalar CRUD, projection ordering, filters and pagination, transactions,
relations, grouping, upserts, complex values, generated integer IDs and duplicate primary keys.
Schema-tool checks exercise introspection, generated migration application, and repeat application.
They are a compatibility probe, not the ORMs' upstream test suites or a performance benchmark.

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

Connection URLs use literal namespace/database slashes, such as
`postgresql://root@127.0.0.1:15432/orm/drizzle?options=-c%20dialect%3Dpgsql`.
The pinned node-postgres connection parser preserves a percent-encoded slash literally, which
would select a different database name. The Prisma runtime uses `@prisma/adapter-pg`; Drizzle uses
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
