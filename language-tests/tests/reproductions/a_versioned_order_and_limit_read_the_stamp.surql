/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	The order an index supplies, and a LIMIT or START cut inside an index scan,
	are answered from entries that hold the state the last write left. Under
	`VERSION` a statement is owed the order and the page of the records at the
	stamp, so each must equal its `WITH NOINDEX` twin, which reads the table at
	the stamp and sorts and cuts what it read.

	After the stamp the smallest value is deleted, a new smallest appears and
	the largest moves to the middle, so an order or a page taken from the index
	differs from the stamp's at every position.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, kind: 'a', rank: 10 }]"

[[test.results]]
value = "[{ id: item:2, kind: 'a', rank: 20 }]"

[[test.results]]
value = "[{ id: item:3, kind: 'a', rank: 30 }]"

[[test.results]]
value = "[{ id: item:4, kind: 'a', rank: 40 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:5, kind: 'a', rank: 5 }]"

[[test.results]]
value = "[{ id: item:4, kind: 'a', rank: 15 }]"

[[test.results]]
value = "[item:1, item:2, item:3, item:4]"

[[test.results]]
value = "[item:1, item:2, item:3, item:4]"

[[test.results]]
value = "[item:4, item:3, item:2, item:1]"

[[test.results]]
value = "[item:4, item:3, item:2, item:1]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[item:3, item:2]"

[[test.results]]
value = "[item:3, item:2]"

[[test.results]]
value = "[item:2, item:3]"

[[test.results]]
value = "[item:2, item:3]"

[[test.results]]
value = "[item:1, item:2, item:3]"

[[test.results]]
value = "[item:1, item:2, item:3]"

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[item:1, item:2]"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE INDEX item_rank ON item FIELDS rank;
DEFINE INDEX item_kind_rank ON item FIELDS kind, rank;

CREATE item:1 SET kind = "a", rank = 10;
CREATE item:2 SET kind = "a", rank = 20;
CREATE item:3 SET kind = "a", rank = 30;
CREATE item:4 SET kind = "a", rank = 40;
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- After the stamp: the smallest rank is deleted, a new smallest appears and
-- the largest moves to the middle.
DELETE item:1;
CREATE item:5 SET kind = "a", rank = 5;
UPDATE item:4 SET rank = 15;

-- An ORDER BY an index supplies, with and without a bound.
SELECT VALUE id FROM item ORDER BY rank VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX ORDER BY rank VERSION $ts;
SELECT VALUE id FROM item ORDER BY rank DESC VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX ORDER BY rank DESC VERSION $ts;
SELECT VALUE id FROM item ORDER BY rank LIMIT 2 VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX ORDER BY rank LIMIT 2 VERSION $ts;
SELECT VALUE id FROM item ORDER BY rank DESC LIMIT 2 START 1 VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX ORDER BY rank DESC LIMIT 2 START 1 VERSION $ts;

-- A predicate and an order on the same index.
SELECT VALUE id FROM item WHERE rank > 10 ORDER BY rank LIMIT 2 VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE rank > 10 ORDER BY rank LIMIT 2 VERSION $ts;

-- A compound index: equality prefix, order on the next column.
SELECT VALUE id FROM item WHERE kind = "a" ORDER BY rank LIMIT 3 VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE kind = "a" ORDER BY rank LIMIT 3 VERSION $ts;

-- A bound with no order, over an indexed predicate.
SELECT VALUE id FROM item WHERE rank >= 10 LIMIT 2 VERSION $ts;
SELECT VALUE id FROM item WITH NOINDEX WHERE rank >= 10 LIMIT 2 VERSION $ts;
