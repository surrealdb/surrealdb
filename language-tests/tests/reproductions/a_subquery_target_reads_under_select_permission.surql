/**
[env]
imports = ["reproductions/a_subquery_target_reads_under_select_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }

[test]
reason = """
An explicitly written nested `SELECT` naming a mutation's subjects is a read,
so the table it reads is gated by `FOR select` — not by the clause the
enclosing statement writes under. A caller granted `FOR update FULL` and
`FOR select NONE` may edit every record of the table and see none of them, and
a subquery target must not turn that into a way to read one: neither through
the rows the statement then reports, nor through a side effect in the
subquery's own projection or condition, which runs only for the rows the
subquery yields.

The clause the statement writes under still gates the targets it names
directly. A bare table target reaches every record for a caller who may update
them, whether or not that caller may select them.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 1]"
*/

-- The subquery reads `t`, which this caller may not select, so it names no
-- rows: the mutation writes nothing and the projection never runs.
UPDATE (SELECT *, (CREATE leak SET seen = secret) AS z FROM t) SET touched = true;
SELECT VALUE seen FROM leak;

-- The same target under `DELETE`, whose clause is as permissive as `update`.
DELETE (SELECT *, (CREATE leak SET seen = secret) AS z FROM t);
SELECT VALUE seen FROM leak;

-- A condition of the subquery is judged after the permission that denies the
-- row, so a side effect written there does not run either.
UPDATE (SELECT id FROM t WHERE (CREATE leak SET seen = secret) != NONE) SET touched = true;
SELECT VALUE seen FROM leak;

-- A subquery target over a table the caller may select still reads it, and the
-- mutation still writes the rows it named.
UPDATE (SELECT *, (CREATE read_ok SET n = 1) AS z FROM u) SET touched = true RETURN NONE;
SELECT VALUE n FROM read_ok;

-- A bare table target is gated by the statement's own clause: the caller may
-- update every record of `t`, and the data clause runs once for each.
UPDATE t SET touched = (CREATE write_ok SET n = 1) RETURN NONE;
SELECT VALUE n FROM write_ok;
