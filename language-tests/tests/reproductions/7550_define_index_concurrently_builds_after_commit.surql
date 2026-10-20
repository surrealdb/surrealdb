/**
[env]
# Extra timeout because this test has a sleep.
timeout = 5000
context-timeout = 5000

[test]
issue = 7550
reason = """
A `DEFINE INDEX … CONCURRENTLY` build reads the table only once its definition
has committed, so it indexes the record values the defining transaction left,
including those written after the `DEFINE` in the same transaction. Writes from
another connection while the defining transaction is open are pinned by
`writes_inside_an_open_concurrent_definition_reach_the_index` in
surrealdb/core/src/kvs/index/tests.rs.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[4, 5]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[4, 5]"

*/
DEFINE TABLE t SCHEMALESS;
FOR $i IN 1..=5 { CREATE type::record('t', $i) SET x = 'old' RETURN NONE; };
BEGIN;
DEFINE INDEX ix ON t FIELDS x CONCURRENTLY;
UPDATE t:1..=3 SET x = 'new' RETURN NONE;
COMMIT;
SLEEP 1s;
SELECT VALUE record::id(id) FROM t WITH INDEX ix WHERE x = 'new' ORDER BY id;
SELECT VALUE record::id(id) FROM t WITH INDEX ix WHERE x = 'old' ORDER BY id;
SELECT VALUE record::id(id) FROM t WITH NOINDEX WHERE x = 'new' ORDER BY id;
SELECT VALUE record::id(id) FROM t WITH NOINDEX WHERE x = 'old' ORDER BY id;
