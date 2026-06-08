/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "`score != NONE` must push down to a BTree index as an exclusive lower bound `> NONE` and return the same rows as the table-scan + filter path (NONE sorts first in BTree key ordering so this is exact). `score != NULL` is intentionally NOT pushed because that would also drop NONE rows even though `NONE != NULL` is true under SurrealQL semantics."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:a }]"

[[test.results]]
value = "[{ id: t:b, score: NULL }]"

[[test.results]]
value = "[{ id: t:c, score: 5 }]"

[[test.results]]
value = "[{ id: t:d, score: 10 }]"

[[test.results]]
value = "[{ id: t:b }, { id: t:c }, { id: t:d }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    IndexScan [ctx: Db] [index: idx_score, access: >NONE, direction: Forward]\n''''

[[test.results]]
value = "[{ id: t:b }, { id: t:c }, { id: t:d }]"

[[test.results]]
value = "[{ id: t:a }, { id: t:c }, { id: t:d }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    TableScan [ctx: Db] [table: t, direction: Forward, predicate: score != NULL, pre_decode_filter: yes]\n''''

[[test.results]]
value = "[{ id: t:a }, { id: t:c }, { id: t:d }]"
*/
DEFINE TABLE t;
DEFINE INDEX idx_score ON t FIELDS score;

CREATE t:a SET score = NONE;
CREATE t:b SET score = NULL;
CREATE t:c SET score = 5;
CREATE t:d SET score = 10;

-- `score != NONE` via index: range scan from > NONE.
SELECT id FROM t WHERE score != NONE ORDER BY id;
EXPLAIN SELECT id FROM t WHERE score != NONE;

-- Index and TableScan paths must agree for `score != NONE`.
SELECT id FROM t WITH NOINDEX WHERE score != NONE ORDER BY id;

-- `score != NULL` falls back to TableScan + filter so NONE rows are kept.
SELECT id FROM t WHERE score != NULL ORDER BY id;
EXPLAIN SELECT id FROM t WHERE score != NULL;

-- Confirm index and TableScan paths agree for `score != NULL`.
SELECT id FROM t WITH NOINDEX WHERE score != NULL ORDER BY id;
