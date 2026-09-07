/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Graph reachability semi-joins as bitmap branches: plan selection, EXPLAIN shape and results"
issue = 549

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
value = "[]"

[[test.results]]
value = "[person:2, person:4]"

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: id]\n    Filter [ctx: Db] [predicate: flag = true AND ->wrote->doc CONTAINS doc:x]\n      predicate.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: wrote, output: TargetVertex]\n          CurrentValueSource [ctx: Rt]\n        BitmapResolve [ctx: Db] [table: person]\n            BitmapAnd [ctx: Db]\n                BitmapIndexScan [ctx: Db] [index: idx_flag, access: = true]\n                BitmapGraphScan [ctx: Db] [source: doc:x, direction: <-, edges: wrote]\n''''

[[test.results]]
value = "[person:2, person:4]"

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: id]\n    TableScan [ctx: Db] [table: person, direction: Forward, predicate: ->wrote->doc CONTAINS doc:x, pre_decode_filter: no (unsupported predicate)]\n''''

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: id]\n    Filter [ctx: Db] [predicate: ->wrote->doc->wrote->doc CONTAINS doc:x]\n      predicate.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: wrote, output: TargetVertex]\n          GraphEdgeScan [ctx: Db] [direction: ->, tables: wrote, output: TargetVertex]\n              CurrentValueSource [ctx: Rt]\n        IndexScan [ctx: Db] [index: idx_flag, access: = true, direction: Forward]\n''''

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: id] {rows: 2}\n    Filter [ctx: Db] [predicate: flag = true AND ->wrote->doc CONTAINS doc:x] {rows: 2}\n      predicate.lookup: GraphEdgeScan [ctx: Db] [direction: ->, tables: wrote, output: TargetVertex] {rows: 2}\n          CurrentValueSource [ctx: Rt] {rows: 2}\n        BitmapResolve [ctx: Db] [table: person] {rows: 2}\n            BitmapAnd [ctx: Db] {rows: 2}\n                BitmapIndexScan [ctx: Db] [index: idx_flag, access: = true] {rows: 3}\n                BitmapGraphScan [ctx: Db] [source: doc:x, direction: <-, edges: wrote] {rows: 4}\n\nTotal rows: 2''''
*/

DEFINE FIELD flag ON person TYPE bool;
DEFINE INDEX idx_flag ON person FIELDS flag;
DEFINE TABLE wrote TYPE RELATION;

CREATE person:1 SET flag = false RETURN NONE;
CREATE person:2 SET flag = true  RETURN NONE;
CREATE person:3 SET flag = false RETURN NONE;
CREATE person:4 SET flag = true  RETURN NONE;
CREATE person:5 SET flag = true  RETURN NONE;
CREATE doc:x RETURN NONE;
CREATE doc:y RETURN NONE;
RELATE person:1->wrote->doc:x RETURN NONE;
RELATE person:2->wrote->doc:x RETURN NONE;
RELATE person:3->wrote->doc:x RETURN NONE;
RELATE person:4->wrote->doc:x RETURN NONE;
RELATE person:5->wrote->doc:y RETURN NONE;

-- An indexed sibling AND a single-hop containment: the traversal anchors at
-- the literal, walks the inverse direction, and intersects as a bitmap.
SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x;
EXPLAIN SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x;

-- The flipped INSIDE spelling.
SELECT VALUE id FROM person WHERE flag = true AND doc:x INSIDE ->wrote->doc;

-- A graph conjunct alone has nothing to anchor the intersection: no bitmap plan.
EXPLAIN SELECT VALUE id FROM person WHERE ->wrote->doc CONTAINS doc:x;

-- Multi-hop traversals keep today's per-row plan.
EXPLAIN SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc->wrote->doc CONTAINS doc:x;

-- EXPLAIN ANALYZE reports the reachable-set cardinality per branch.
EXPLAIN ANALYZE SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x;
