# Upstream ORM suites against the SurrealDB PostgreSQL listener

Control: PostgreSQL 18.4 (embedded). "SurrealDB pass rate" is tests passing on both over tests passing on the reference.

## drizzle / pg (0.45.4)

| | pass | fail | skip |
|---|---|---|---|
| surrealdb | 0 | 854 | 0 |
| reference | 854 | 0 | 0 |

| classification | tests |
|---|---|
| pass_both | 0 |
| surrealdb_only | 854 |
| not_run_surrealdb | 0 |
| fail_both | 0 |
| reference_only | 0 |
| skipped | 5 |

SurrealDB pass rate: 0.0%

**Top causes, fails only on SurrealDB**

| count | cause |
|---|---|
| 669 | 0A000: Failed query: drop schema if exists public cascade |
| 184 | 0A000: Failed query: drop schema public cascade |
| 1 | 42601: Failed query: select "…"."…", "…"."…", "…"."…", "…"."…", "…"."…" as "…", "…"."…" as "…" from "…" "…" left join lateral (select coalesce(json |

## drizzle-kit / kit-pg (0.31.11)

| | pass | fail | skip |
|---|---|---|---|
| surrealdb | 1 | 121 | 0 |
| reference | 121 | 1 | 0 |

| classification | tests |
|---|---|
| pass_both | 1 |
| surrealdb_only | 120 |
| not_run_surrealdb | 0 |
| fail_both | 1 |
| reference_only | 0 |
| skipped | 1 |

SurrealDB pass rate: 0.8%

**Top causes, fails only on SurrealDB**

| count | cause |
|---|---|
| 62 | 42601: PostgreSQL parse error: at N..N: unexpected token: expected "…", found keyword "…" |
| 30 | 42P01: The table "…" does not exist |
| 20 | 42601: PostgreSQL parse error: at N..N: unexpected token: expected ADD, DROP, ALTER, or RENAME, found keyword "…" |
| 4 | 23503: PostgreSQL parse error: at N..N: unexpected token: expected PRIMARY KEY, UNIQUE, or FOREIGN KEY, found keyword "…" |
| 2 | 42601: PostgreSQL parse error: at N..N: unexpected token: expected ;, found keyword "…" |
| 1 | 42601: PostgreSQL parse error: at N..N: unexpected token: expected "…", found identifier "…" |
| 1 | XX000: Cannot change field "…" of table "…": its DEFAULT does not coerce to "…" |

**Top causes, fails on both**

| count | cause |
|---|---|
| 1 | 42P01: The table "…" does not exist |

## prisma / client-functional (7.10.1)

| | pass | fail | skip |
|---|---|---|---|
| surrealdb | 10 | 1328 | 0 |
| reference | 10 | 5 | 0 |

1323 test(s) or shard(s) were stopped by the ORM's own AI-agent safety guard before touching the database and are excluded; export the guard variable yourself to run them.

| classification | tests |
|---|---|
| pass_both | 10 |
| surrealdb_only | 1323 |
| not_run_surrealdb | 0 |
| fail_both | 5 |
| reference_only | 0 |
| skipped | 25691 |

NOT MEASURED: the safety guard blocked most of this suite, so the buckets above do not compare like with like.

**Top causes, fails only on SurrealDB**

| count | cause |
|---|---|
| 1323 | Error: Schema engine error: |

**Top causes, fails on both**

| count | cause |
|---|---|
| 4 | {"…":"…","…":"ReferenceError: prisma is not defined\n    at Object.<anonymous> (/Users/tobie/Repositories/surrealdb/.claude/worktrees/destru |
| 1 | {"…":"…","…":"TypeError: Cannot read properties of undefined (reading "…")\n    at Object.<anonymous> (/Users/tobie/Repositories/surrealdb/. |

## prisma / migrate-postgres (7.10.1)

| | pass | fail | skip |
|---|---|---|---|
| surrealdb | 9 | 29 | 0 |
| reference | 38 | 19 | 0 |

1 test(s) or shard(s) were stopped by the ORM's own AI-agent safety guard before touching the database and are excluded; export the guard variable yourself to run them.

| classification | tests |
|---|---|
| pass_both | 9 |
| surrealdb_only | 17 |
| not_run_surrealdb | 13 |
| fail_both | 12 |
| reference_only | 7 |
| skipped | 1 |

SurrealDB pass rate: 23.1%

**Top causes, fails only on SurrealDB**

| count | cause |
|---|---|
| 10 | Error: expect(received).resolves.toMatchInlineSnapshot() |
| 3 | Error: expect(received).resolves.toContain() |
| 1 | Error: expect(received).toContain(expected) // indexOf |
| 1 | Error: expect(received).toEqual(expected) // deep equality |
| 1 | Error: expect(received).rejects.toThrowErrorMatchingInlineSnapshot(snapshot) |
| 1 | Error: expect(received).toMatchInlineSnapshot(snapshot) |

**Top causes, fails on both**

| count | cause |
|---|---|
| 11 | Error: expect(received).resolves.toMatchInlineSnapshot() |
| 1 | Error: expect(received).rejects.toMatchInlineSnapshot(snapshot) |

## typeorm / functional (1.1.2)

| | pass | fail | skip |
|---|---|---|---|
| surrealdb | 538 | 448 | 0 |
| reference | 2317 | 9 | 0 |

| classification | tests |
|---|---|
| pass_both | 538 |
| surrealdb_only | 443 |
| not_run_surrealdb | 1774 |
| fail_both | 5 |
| reference_only | 4 |
| skipped | 155 |

SurrealDB pass rate: 19.5%

**Top causes, fails only on SurrealDB**

| count | cause |
|---|---|
| 436 | 42601: at N..N: unexpected token: expected "…", found parameter $N |
| 4 | Cannot read properties of undefined (reading "…") |
| 1 | 42601: PostgreSQL parse error: at N..N: unexpected token: expected TABLE, INDEX, VIEW, SCHEMA, TYPE, or PROPERTY GRAPH, found keyword "…" |
| 1 | 42601: PostgreSQL parse error: at N..N: unexpected token: expected "…", found keyword "…" |
| 1 | 3D000: Internal database error: IR table full: vm registers |

**Never ran on SurrealDB because a setup hook failed, by cause**

| count | cause |
|---|---|
| 1744 | 42601: at N..N: unexpected token: expected "…", found parameter $N |
| 18 | Cannot read properties of undefined (reading "…") |
| 9 | 42601: PostgreSQL parse error: at N..N: unexpected token: expected "…", found keyword "…" |
| 3 | 3D000: Internal database error: IR table full: vm registers |

**Top causes, fails on both**

| count | cause |
|---|---|
| 5 | 42601: at N..N: unexpected token: expected "…", found parameter $N |

