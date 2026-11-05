/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	An index entry carries no version stamp: it holds the state the last write
	left. A read under `VERSION` is owed the records as they stood at the stamp,
	so every access path answered from an index entry is wrong for it, and the
	read must equal its `WITH NOINDEX` twin, which scans the table at the stamp.

	After the stamp one record appears, one changes every field an index covers,
	one is deleted and one moves into another key. Each shape of B-tree access
	is asked beside its `WITH NOINDEX` twin, and each pair must agree: an
	equality, a range, a compound prefix, a unique lookup, a union of lookups
	and an intersection.
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
value = "[{ code: 'x1', id: item:1, kind: 'a', rank: 1 }]"

[[test.results]]
value = "[{ code: 'x2', id: item:2, kind: 'a', rank: 2 }]"

[[test.results]]
value = "[{ code: 'x3', id: item:3, kind: 'b', rank: 3 }]"

[[test.results]]
value = "[{ code: 'x4', id: item:4, kind: 'a', rank: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ code: 'x5', id: item:5, kind: 'a', rank: 5 }]"

[[test.results]]
value = "[{ code: 'y1', id: item:1, kind: 'b', rank: 9 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ code: 'x3', id: item:3, kind: 'a', rank: 1 }]"

[[test.results]]
value = "[item:1, item:2, item:4]"

[[test.results]]
value = "[item:1, item:2, item:4]"

[[test.results]]
value = "[item:2, item:3, item:4]"

[[test.results]]
value = "[item:2, item:3, item:4]"

[[test.results]]
value = "[item:2, item:4]"

[[test.results]]
value = "[item:2, item:4]"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[item:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[item:2, item:3]"

[[test.results]]
value = "[item:2, item:3]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[item:1, item:2]"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE INDEX item_kind ON item FIELDS kind;
DEFINE INDEX item_rank ON item FIELDS rank;
DEFINE INDEX item_pair ON item FIELDS kind, rank;
DEFINE INDEX item_code ON item FIELDS code UNIQUE;

CREATE item:1 SET kind = "a", rank = 1, code = "x1";
CREATE item:2 SET kind = "a", rank = 2, code = "x2";
CREATE item:3 SET kind = "b", rank = 3, code = "x3";
CREATE item:4 SET kind = "a", rank = 4, code = "x4";
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- After the stamp: a matching record appears, an indexed field changes,
-- a record is deleted.
CREATE item:5 SET kind = "a", rank = 5, code = "x5";
UPDATE item:1 SET kind = "b", rank = 9, code = "y1";
DELETE item:2;
UPDATE item:3 SET kind = "a", rank = 1;

-- Equality on a single-column index.
SELECT VALUE id FROM item WHERE kind = "a" ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE kind = "a" ORDER BY id VERSION $ts;

-- A range on a single-column index.
SELECT VALUE id FROM item WHERE rank >= 2 AND rank < 5 ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE rank >= 2 AND rank < 5 ORDER BY id VERSION $ts;

-- A compound index, equality prefix and range.
SELECT VALUE id FROM item WHERE kind = "a" AND rank > 1 ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE kind = "a" AND rank > 1 ORDER BY id VERSION $ts;

-- A unique index.
SELECT VALUE id FROM item WHERE code = "x1" VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE code = "x1" VERSION $ts;
SELECT VALUE id FROM item WHERE code = "y1" VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE code = "y1" VERSION $ts;

-- Several lookups unioned: IN and OR.
SELECT VALUE id FROM item WHERE code IN ["x1", "x2", "x5"] ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE code IN ["x1", "x2", "x5"] ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WHERE kind = "b" OR rank = 2 ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE kind = "b" OR rank = 2 ORDER BY id VERSION $ts;

-- Several indexes ANDed.
SELECT VALUE id FROM item WHERE kind = "a" AND code != "x4" ORDER BY id VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE kind = "a" AND code != "x4" ORDER BY id VERSION $ts;
