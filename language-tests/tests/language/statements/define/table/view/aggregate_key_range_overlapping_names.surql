/**
[env]
namespace = true
database = true

[test]
reason = """
A view row stores each select item under its name, in order, so where two items
write the same name, or one writes beneath the other's name, the row holds there
what the last of them wrote, which need not be the group value the row's key
holds. Such a name, and a name that is `id` or beneath it (a read of `id` reads
the row's record id), does not address a component of the key: a predicate on it
keeps the scan, and every result is what `WITH NOINDEX` gives. A key range over
`id[i]` itself is unaffected.
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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 'a', id: dup:['m', 'a'], n: 1 }, { a: 'a', id: dup:['zz', 'a'], n: 1 }, { a: 'q', id: dup:['A', 'q'], n: 1 }]"

[[test.results]]
value = "[dup:['m', 'a'], dup:['zz', 'a']]"

[[test.results]]
value = "[dup:['m', 'a'], dup:['zz', 'a']]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: dup, direction: Forward, predicate: a < 'b', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[nested:['A', 'q']]"

[[test.results]]
value = "[nested:['A', 'q']]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: nested, direction: Forward, predicate: loc = 'A' AND loc.r < 'a', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[replaced:['A'], replaced:['m'], replaced:['zz']]"

[[test.results]]
value = "[replaced:['A'], replaced:['m'], replaced:['zz']]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: replaced, direction: Forward, predicate: a > 'z', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[named_id:['A'], named_id:['m'], named_id:['zz']]"

[[test.results]]
value = "[named_id:['A'], named_id:['m'], named_id:['zz']]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: id > 'b']
        TableScan [ctx: Db] [table: named_id, direction: Forward]
"'''

[[test.results]]
value = "[beneath_id:['A'], beneath_id:['m'], beneath_id:['zz']]"

[[test.results]]
value = "[beneath_id:['A'], beneath_id:['m'], beneath_id:['zz']]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: id[0] = 'm']
        KeyRangeScan [ctx: Db] [table: dup, key: id[0] = 'm', direction: Forward, residual: yes]
"'''

[[test.results]]
value = "[dup:['m', 'a']]"
*/
DEFINE TABLE src;
DEFINE TABLE dup AS SELECT g1 AS a, g2 AS a, count() AS n FROM src GROUP BY g1, g2;
DEFINE TABLE nested AS SELECT g2 AS loc.r, g1 AS loc, count() AS n FROM src GROUP BY g1, g2;
DEFINE TABLE replaced AS SELECT g1 AS a, { r: 1 } AS a, count() AS n FROM src GROUP BY g1;
DEFINE TABLE named_id AS SELECT g1 AS id, count() AS n FROM src GROUP BY g1;
DEFINE TABLE beneath_id AS SELECT g1 AS id.x, count() AS n FROM src GROUP BY g1;
CREATE src:1 SET g1 = 'A', g2 = 'q' RETURN NONE;
CREATE src:2 SET g1 = 'm', g2 = 'a' RETURN NONE;
CREATE src:3 SET g1 = 'zz', g2 = 'a' RETURN NONE;
SELECT * FROM dup ORDER BY n, a;
-- `a` holds g2, the key's second element: a range over the first would miss rows.
SELECT VALUE id FROM dup WHERE a < 'b' ORDER BY id;
SELECT VALUE id FROM dup WITH NOINDEX WHERE a < 'b' ORDER BY id;
EXPLAIN SELECT VALUE id FROM dup WHERE a < 'b';
-- `loc.r` is overwritten by `loc`: it reads NONE, which sorts before every string.
SELECT VALUE id FROM nested WHERE loc = 'A' AND loc.r < 'a';
SELECT VALUE id FROM nested WITH NOINDEX WHERE loc = 'A' AND loc.r < 'a';
EXPLAIN SELECT VALUE id FROM nested WHERE loc = 'A' AND loc.r < 'a';
-- `a` holds the object the second item wrote.
SELECT VALUE id FROM replaced WHERE a > 'z' ORDER BY id;
SELECT VALUE id FROM replaced WITH NOINDEX WHERE a > 'z' ORDER BY id;
EXPLAIN SELECT VALUE id FROM replaced WHERE a > 'z';
-- `id` is the row's record id, which sorts after every string.
SELECT VALUE id FROM named_id WHERE id > 'b' ORDER BY id;
SELECT VALUE id FROM named_id WITH NOINDEX WHERE id > 'b' ORDER BY id;
EXPLAIN SELECT VALUE id FROM named_id WHERE id > 'b';
SELECT VALUE id FROM beneath_id WHERE id.x < 'b' ORDER BY id;
SELECT VALUE id FROM beneath_id WITH NOINDEX WHERE id.x < 'b' ORDER BY id;
-- The key itself still plans a range.
EXPLAIN SELECT VALUE id FROM dup WHERE id[0] = 'm';
SELECT VALUE id FROM dup WHERE id[0] = 'm';
