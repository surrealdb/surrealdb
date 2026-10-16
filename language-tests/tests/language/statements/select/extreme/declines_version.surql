/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	The extreme of an indexed `int` field read under `VERSION` is the extreme of the rows as they
	stood at that version. An index entry carries no version, so the read is never answered from
	the index: a row written, changed or removed after the version does not move the answer.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ m: -2 }]"

[[test.results]]
value = "[9]"

[[test.results]]
value = "[-2]"

[[test.results]]
value = "9"

[[test.results]]
value = "-2"

[[test.results]]
value = "[{ m: 9 }]"

[[test.results]]
value = "[{ id: t:2, m: 9 }, { id: t:3, m: 9 }, { id: t:4, m: 9 }, { id: t:5, m: 9 }]"

[[test.results]]
value = "[{ m: 40 }]"

[[test.results]]
value = "[{ m: -40 }]"

[[test.results]]
value = "40"

[[test.results]]
error = "The table 't' does not exist"

[[test.results]]
error = "The table 't' does not exist"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE int;
DEFINE INDEX n ON t FIELDS n;
CREATE t:1 SET n = 5 RETURN NONE;
CREATE t:2 SET n = 9 RETURN NONE;
CREATE t:3 SET n = -2 RETURN NONE;
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
-- Rows that hold both extremes after the version.
CREATE t:4 SET n = 40 RETURN NONE;
CREATE t:5 SET n = -40 RETURN NONE;
UPDATE t:2 SET n = 3 RETURN NONE;
DELETE t:1 RETURN NONE;
SELECT math::max(n) AS m FROM t GROUP ALL VERSION $ts;
SELECT math::min(n) AS m FROM t GROUP ALL VERSION $ts;
SELECT VALUE math::max(n) FROM t GROUP ALL VERSION $ts;
SELECT VALUE math::min(n) FROM t GROUP ALL VERSION $ts;
RETURN (SELECT VALUE math::max(n) FROM t GROUP ALL VERSION $ts)[0];
RETURN (SELECT VALUE math::min(n) FROM t GROUP ALL VERSION $ts)[0];
SELECT math::max(n) AS m FROM t GROUP ALL LIMIT 1 VERSION $ts;
SELECT id, (SELECT VALUE math::max(n) FROM t GROUP ALL VERSION $ts)[0] AS m FROM t ORDER BY id;
-- The same statements at the present.
SELECT math::max(n) AS m FROM t GROUP ALL;
SELECT math::min(n) AS m FROM t GROUP ALL;
RETURN (SELECT VALUE math::max(n) FROM t GROUP ALL)[0];
-- A version before every row.
SELECT math::max(n) AS m FROM t GROUP ALL VERSION d'2020-01-01T00:00:00Z';
SELECT math::min(n) AS m FROM t GROUP ALL VERSION d'2020-01-01T00:00:00Z';
