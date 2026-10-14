/**
[env]

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
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: flag = true AND ->wrote->doc CONTAINS (doc:x)]
        BitmapScan [ctx: Db] [table: person]
            BitmapAnd [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: idx_flag, access: = true]
                BitmapGraphScan [ctx: Db] [source: doc:x, direction: <-, edges: wrote]
''''

[[test.results]]
value = "[person:2, person:4]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: ->wrote->doc CONTAINS (doc:x)]
        TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: ->wrote->doc->wrote->doc CONTAINS (doc:x)]
        IndexScan [ctx: Db] [table: person, index: idx_flag, access: = true, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id] {rows: 2}
    Filter [ctx: Db] [predicate: flag = true AND ->wrote->doc CONTAINS (doc:x)] {rows: 2}
        BitmapScan [ctx: Db] [table: person] {rows: 2}
            BitmapAnd [ctx: Db] {rows: 2}
                BitmapIndexScan [ctx: Db] [index: idx_flag, access: = true] {rows: 3}
                BitmapGraphScan [ctx: Db] [source: doc:x, direction: <-, edges: wrote] {rows: 4}

Total rows: 2''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        Filter [ctx: Db] [predicate: flag = true AND ->wrote->doc CONTAINS (doc:x)]
            BitmapScan [ctx: Db] [table: person]
                BitmapAnd [ctx: Db]
                    BitmapIndexScan [ctx: Db] [index: idx_flag, access: = true]
                    BitmapGraphScan [ctx: Db] [source: doc:x, direction: <-, edges: wrote]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1}
        Filter [ctx: Db] [predicate: flag = true AND ->wrote->doc CONTAINS (doc:x)] {rows: 1}
            BitmapScan [ctx: Db] [table: person] {rows: 1}
                BitmapAnd [ctx: Db] {rows: 2}
                    BitmapIndexScan [ctx: Db] [index: idx_flag, access: = true] {rows: 3}
                    BitmapGraphScan [ctx: Db] [source: doc:x, direction: <-, edges: wrote] {rows: 4}

Total rows: 1''''
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

-- A count over the same conjunction takes the bitmap plan too, so the aggregate never visits a
-- row the reachable set does not hold.
SELECT count() FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x GROUP ALL;
EXPLAIN SELECT count() FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x GROUP ALL;
EXPLAIN ANALYZE SELECT count() FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x GROUP ALL;
