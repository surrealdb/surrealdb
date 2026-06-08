/**
[test]
reason = "Plan-time cycle guard prevents infinite recursion when a SELECT permission references the same table via a subquery; outer plan compiles fully and execution returns correct results."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: gated:a, label: 'alpha', tag: 'public' }, { id: gated:b, label: 'beta', tag: 'public' }, { id: gated:c, label: 'private', tag: 'private' }]"

[[test.results]]
value = "[{ id: gated:a, label: 'alpha', tag: 'public' }, { id: gated:b, label: 'beta', tag: 'public' }]"

[[test.results]]
value = "[{ id: gated:a, label: 'alpha', tag: 'public' }]"

[env]
namespace = true
database = true
auth = { level = "owner" }

*/

-- Permission references the same table via a subquery. Without a plan-time
-- cycle guard, threading a planner-with-txn through permission compilation
-- re-enters resolve_table_context for `gated` infinitely. With the guard,
-- the *outer* plan resolves `gated` fully, while the inner subquery on
-- `gated` falls back to runtime resolution.
{
    DEFINE TABLE gated SCHEMAFULL
        PERMISSIONS FOR SELECT WHERE
            tag IN (SELECT VALUE tag FROM gated WHERE tag = 'public');

    DEFINE FIELD label ON gated TYPE string;
    DEFINE FIELD tag ON gated TYPE string;

    CREATE gated:a SET label = 'alpha', tag = 'public';
    CREATE gated:b SET label = 'beta', tag = 'public';
    CREATE gated:c SET label = 'private', tag = 'private';

    RETURN 'OK';
};

-- Owner-level: permissions are skipped at runtime, but the plan-time
-- compile path still resolves the table context (which compiles the
-- self-referential permission). Without the cycle guard, this overflows
-- the stack at planning time.
SELECT * FROM gated ORDER BY id;

-- WHERE-filtered query: same compile path, but the outer scan now has a
-- predicate. The predicate's compilation also goes through `physical_expr`
-- which inherits the cycle guard.
SELECT * FROM gated WHERE tag = 'public' ORDER BY id;

-- Specific record: exercises the RecordIdScan path that also calls
-- try_resolve_table_ctx for the table.
SELECT * FROM gated:a;
