/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	A read under `VERSION` chooses its access path among the indexes its table
	declared at the stamp. An index defined after the stamp lists the records
	as they are now, so a record that matched at the stamp and has changed
	since is not among them: the records the table held are read by scanning
	it, which reads each at the stamp.

	An index that exists at the stamp and still exists is chosen as it is for
	any read.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, kind: 'a', size: 1 }]"

[[test.results]]
value = "[{ id: item:2, kind: 'b', size: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:3, kind: 'a', size: 2 }]"

[[test.results]]
value = "[{ id: item:1, kind: 'a', size: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1 }]"

[[test.results]]
value = "[{ id: item:1 }, { id: item:3 }]"

[[test.results]]
value = "[{ id: item:1 }, { id: item:2 }]"

[[test.results]]
value = "[{ id: item:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:1 }]"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE INDEX item_kind ON item FIELDS kind;
CREATE item:1 SET kind = "a", size = 1;
CREATE item:2 SET kind = "b", size = 1;
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
CREATE item:3 SET kind = "a", size = 2;
UPDATE item:1 SET size = 5;
DEFINE INDEX item_size ON item FIELDS size;

-- Declared at the stamp.
SELECT id FROM item WHERE kind = "a" ORDER BY id VERSION $ts;
SELECT id FROM item WHERE kind = "a" ORDER BY id;

-- Declared after the stamp: item:1 held size 1 then and holds 5 now.
SELECT id FROM item WHERE size = 1 ORDER BY id VERSION $ts;
SELECT id FROM item WHERE size = 1 ORDER BY id;
SELECT id FROM item WHERE size = 5 ORDER BY id VERSION $ts;
SELECT id FROM item WHERE size = 5 ORDER BY id;
