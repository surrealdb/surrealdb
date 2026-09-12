/**
[env]
backend = ["rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	A count under `VERSION` counts what the database held at that stamp.

	A count is answered without materialising a row wherever it can be: from
	the key range, from a `COUNT` index's deltas, from a B-tree index's entries
	or from a fused bitmap over several indexes. None of those is stamped —
	they are the state the last write left — so a versioned count is answered
	by the scan instead, which reads at the stamp.

	Each shape is asked twice, once for now and once for the stamp taken before
	the second record was written, and the pair is the assertion: an
	index-assisted count that ignored the stamp would report both as 2. The
	last pair says the same of the records an indexed predicate selects, which
	is the wider rule: an index answers no versioned read.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, kind: 'a', status: 'active' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:2, kind: 'a', status: 'active' }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ id: item:1 }, { id: item:2 }]"

[[test.results]]
value = "[{ id: item:1 }]"
































*/

DEFINE TABLE item SCHEMALESS;
DEFINE INDEX item_active_count ON item COUNT WHERE status = "active";
DEFINE INDEX item_kind ON item FIELDS kind;

CREATE item:1 SET status = "active", kind = "a";
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
CREATE item:2 SET status = "active", kind = "a";

-- A COUNT index whose guard is the predicate.
SELECT count() FROM item WHERE status = "active" GROUP ALL;
SELECT count() FROM item WHERE status = "active" GROUP ALL VERSION $ts;

-- A B-tree index covering the predicate.
SELECT count() FROM item WHERE kind = "a" GROUP ALL;
SELECT count() FROM item WHERE kind = "a" GROUP ALL VERSION $ts;

-- The whole key range, with no predicate to index.
SELECT count() FROM item GROUP ALL;
SELECT count() FROM item GROUP ALL VERSION $ts;

-- Control: a predicate no index answers, which was always read at the stamp.
SELECT count() FROM item WHERE status != "gone" GROUP ALL;
SELECT count() FROM item WHERE status != "gone" GROUP ALL VERSION $ts;

-- An index answers no versioned read, not only a count: the records an
-- indexed predicate selects are read at the stamp too.
SELECT id FROM item WHERE kind = "a";
SELECT id FROM item WHERE kind = "a" VERSION $ts;
