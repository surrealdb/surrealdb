/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_compound_ranges_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = """
A read over a compound index pins its leading column and bounds the next one,
so its key range starts or ends inside the run of entries that share the
leading value. Each comparison finds exactly the entries the old writer laid
out on its side of the bound, across numbers of mixed kinds, and none under a
neighbouring leading value, including one that extends the pinned string. An
index that cannot be read still returns these rows through a table scan; its
plan is asserted in the _current sibling."""

[[test.results]]
value = "[cmp:5, cmp:6]"

[[test.results]]
value = "[cmp:2, cmp:3, cmp:4]"

[[test.results]]
value = "[cmp:2, cmp:3, cmp:4, cmp:5, cmp:6]"

[[test.results]]
value = "[cmp:7, cmp:8]"

[[test.results]]
value = "[cmp:5, cmp:6, cmp:7, cmp:8]"

[[test.results]]
value = "[cmp:2, cmp:3, cmp:4, cmp:5, cmp:6, cmp:7, cmp:8]"

[[test.results]]
value = "[cmp:10]"

[[test.results]]
value = "[cmpu:5]"

[[test.results]]
value = "[cmpu:2, cmpu:3, cmpu:4]"

[[test.results]]
value = "[cmpu:2, cmpu:3, cmpu:4, cmpu:5]"

[[test.results]]
value = "[cmpu:7, cmpu:8]"

[[test.results]]
value = "[cmpu:5, cmpu:7, cmpu:8]"

[[test.results]]
value = "[cmpu:2, cmpu:3, cmpu:4, cmpu:5, cmpu:7, cmpu:8]"

[[test.results]]
value = "[cmpu:10]"

[[test.results]]
error = "Database index `cmpu_ab` already contains ['x', 1f], with record `cmpu:5`"

*/
-- Non-unique: the leading value pinned, the next compared each way.
SELECT VALUE id FROM cmp WHERE a = 'x' AND b = 1 ORDER BY id;
SELECT VALUE id FROM cmp WHERE a = 'x' AND b < 1 ORDER BY id;
SELECT VALUE id FROM cmp WHERE a = 'x' AND b <= 1 ORDER BY id;
SELECT VALUE id FROM cmp WHERE a = 'x' AND b > 1 ORDER BY id;
SELECT VALUE id FROM cmp WHERE a = 'x' AND b >= 1 ORDER BY id;
-- Non-unique: the leading value alone, which a longer string extending it
-- must stay out of.
SELECT VALUE id FROM cmp WHERE a = 'x' ORDER BY id;
SELECT VALUE id FROM cmp WHERE a = 'xx' AND b >= 1 ORDER BY id;

-- Unique: the same reads.
SELECT VALUE id FROM cmpu WHERE a = 'x' AND b = 1 ORDER BY id;
SELECT VALUE id FROM cmpu WHERE a = 'x' AND b < 1 ORDER BY id;
SELECT VALUE id FROM cmpu WHERE a = 'x' AND b <= 1 ORDER BY id;
SELECT VALUE id FROM cmpu WHERE a = 'x' AND b > 1 ORDER BY id;
SELECT VALUE id FROM cmpu WHERE a = 'x' AND b >= 1 ORDER BY id;
SELECT VALUE id FROM cmpu WHERE a = 'x' ORDER BY id;
SELECT VALUE id FROM cmpu WHERE a = 'xx' AND b >= 1 ORDER BY id;

-- Unique: a tuple equal in value to one the old binary wrote collides with it.
CREATE cmpu:20 SET a = 'x', b = 1f;
