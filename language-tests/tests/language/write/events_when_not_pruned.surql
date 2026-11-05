/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An event whose `WHEN` is outside the forms a write decides ahead of time runs
exactly as it did. `0` and `""` are falsy but are not `false`, `NONE` or `NULL`; a
call, a read of the written record and a clause that raises are evaluated for every
record. The audit rows say which events ran, and the error of a raising clause is the
one the statement reports, the audit rows of the events before it rolled back.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ flag: true, id: np:1 }]"

[[test.results]]
value = "[{ flag: false, id: np:1 }]"

[[test.results]]
value = "[{ flag: true, id: np:1 }]"

[[test.results]]
value = "[{ flag: true, id: np:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ act: 'CREATE', count: 2, tag: 'call' }, { act: 'DELETE', count: 1, tag: 'call' }, { act: 'UPDATE', count: 2, tag: 'call' }, { act: 'CREATE', count: 2, tag: 'field' }, { act: 'UPDATE', count: 1, tag: 'field' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event b_raises: Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Error while processing event b_raises: Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Error while processing event b_raises: Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE TABLE np SCHEMALESS;
DEFINE TABLE audit SCHEMALESS;
DEFINE FUNCTION fn::f() { RETURN true };
DEFINE EVENT zero ON np WHEN 0 THEN (CREATE audit SET tag = 'zero', act = $event);
DEFINE EVENT empty ON np WHEN "" THEN (CREATE audit SET tag = 'empty', act = $event);
DEFINE EVENT call ON np WHEN fn::f() THEN (CREATE audit SET tag = 'call', act = $event);
DEFINE EVENT field ON np WHEN $after.flag THEN (CREATE audit SET tag = 'field', act = $event);
CREATE np:1 SET flag = true;
UPDATE np:1 SET flag = false;
UPDATE np:1 SET flag = true;
UPSERT np:2 SET flag = true;
DELETE np:1;
SELECT tag, act, count() FROM audit GROUP BY tag, act ORDER BY tag, act;
DEFINE TABLE boom SCHEMALESS;
DEFINE EVENT a_before ON boom WHEN true THEN (CREATE audit SET tag = 'a_before', act = $event);
DEFINE EVENT b_raises ON boom WHEN <int> "x" THEN (CREATE audit SET tag = 'b_raises', act = $event);
DEFINE EVENT c_after ON boom WHEN true THEN (CREATE audit SET tag = 'c_after', act = $event);
DEFINE EVENT d_dormant ON boom WHEN false THEN (CREATE audit SET tag = 'd_dormant', act = $event);
CREATE boom:1;
UPDATE boom:1 SET a = 1;
UPSERT boom:1 SET a = 2;
DELETE boom:1;
INSERT INTO boom { id: boom:2 };
SELECT * FROM boom;
SELECT tag, act, count() FROM audit WHERE tag INSIDE ['a_before', 'b_raises', 'c_after', 'd_dormant'] GROUP BY tag, act ORDER BY tag, act;
