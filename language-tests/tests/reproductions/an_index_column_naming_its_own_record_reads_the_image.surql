/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An index column and a COUNT guard are evaluated once per image of the record
being written, and either may name the record's own id — `FIELDS id.v`,
`COUNT WHERE id.v = 7`. That dereference must answer from the image under
test, not from storage.

The before-image evaluation decides which entry to remove and the after-image
evaluation decides which to write, and both run after the record has been
stored. Answering from storage makes the two images identical, so the entry is
never moved: a lookup through the index then reports the record under its old
key, while the same query without the index reports the truth.

Both directions of the COUNT guard are pinned below, because reading storage
makes the count stale in one direction and wrong in the other depending on
which image the guard happened to accept.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:1, v: 7 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[p:1]"

[[test.results]]
value = "[9]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"
*/
-- A column that dereferences the record's own id. The entry is written by the
-- write path, then must be moved by it.
DEFINE TABLE p SCHEMALESS;
CREATE p:1 SET v = 7;
DEFINE INDEX p_i ON p FIELDS id.v;
SELECT VALUE id FROM p WHERE id.v = 7;
UPDATE p:1 SET v = 9 RETURN NONE;
SELECT VALUE id FROM p WHERE id.v = 9;
SELECT VALUE v FROM p;

-- A COUNT guard that dereferences the record's own id, leaving the count.
DEFINE TABLE d SCHEMALESS;
DEFINE INDEX d_i ON d COUNT WHERE id.v = 7;
CREATE d:1 SET v = 7 RETURN NONE;
SELECT count() FROM d WHERE id.v = 7 GROUP ALL;
UPDATE d:1 SET v = 8 RETURN NONE;
SELECT count() FROM d WHERE id.v = 7 GROUP ALL;
SELECT count() FROM d WHERE v = 7 GROUP ALL;

-- The same guard, entering the count.
DEFINE TABLE k SCHEMALESS;
DEFINE INDEX k_i ON k COUNT WHERE id.v = 9;
CREATE k:1 SET v = 7 RETURN NONE;
SELECT count() FROM k WHERE id.v = 9 GROUP ALL;
UPDATE k:1 SET v = 9 RETURN NONE;
SELECT count() FROM k WHERE id.v = 9 GROUP ALL;
SELECT count() FROM k WHERE v = 9 GROUP ALL;
