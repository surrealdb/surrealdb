/**
[env]

[test]
reason = """A float equals a decimal only when the decimal spells the float's exact value, \
the value its index key spells: `0.1f` is `0.100000000000000005551115123125782702118`, above \
`0.1dec`, while `0.5f` and `0.5dec` are one value. An indexed read and a scan therefore \
answer the same records for an equality or an inclusive bound on either of such a pair, \
`array::distinct` keeps `0.1f` and `0.1dec` apart, and so does a set, in the engine and \
once returned. Numeric expectations compare by value, so a float and a decimal that are \
one number match either kind: the comparisons are asserted as booleans, the reads as \
record ids, and the returned sets by what they hold."""
issue = 1336

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: f:1, v: 0.1f }]"

[[test.results]]
value = "[{ id: f:2, v: 0.1dec }]"

[[test.results]]
value = "[{ id: f:3, v: 0.5f }]"

[[test.results]]
value = "[{ id: f:4, v: 0.5dec }]"

[[test.results]]
value = "[false, true, true, false, true, true]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[f:1]"

[[test.results]]
value = "[f:1]"

[[test.results]]
value = "[f:2]"

[[test.results]]
value = "[f:2]"

[[test.results]]
value = "[f:1, f:3, f:4]"

[[test.results]]
value = "[f:1, f:3, f:4]"

[[test.results]]
value = "[f:2]"

[[test.results]]
value = "[f:2]"

[[test.results]]
value = "[f:3, f:4]"

[[test.results]]
value = "[f:3, f:4]"

[[test.results]]
value = "[f:2, f:3, f:4]"

[[test.results]]
value = "[f:2, f:3, f:4]"

[[test.results]]
value = "[2, 1]"

[[test.results]]
match = "set::len($result) = 2 AND $result CONTAINS 0.1f AND $result CONTAINS 0.1dec"

[[test.results]]
match = "set::len($result) = 1 AND $result CONTAINS 0.5f AND $result CONTAINS 0.5dec"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: f, index: ix, access: = 0.1f, direction: Forward]
''''
*/

DEFINE INDEX ix ON f FIELDS v;
CREATE f:1 SET v = 0.1f;
CREATE f:2 SET v = 0.1dec;
CREATE f:3 SET v = 0.5f;
CREATE f:4 SET v = 0.5dec;

RETURN [0.1f = 0.1dec, 0.1f > 0.1dec, 0.3f < 0.3dec, 1.1f = 1.1dec, 0.5f = 0.5dec, 1f = 1dec];
RETURN 0.00000095367431640625f = 0.00000095367431640625dec;
RETURN <decimal> 0.1f = 0.1dec;

SELECT VALUE id FROM f WITH NOINDEX WHERE v = 0.1f;
SELECT VALUE id FROM f WHERE v = 0.1f;
SELECT VALUE id FROM f WITH NOINDEX WHERE v = 0.1dec;
SELECT VALUE id FROM f WHERE v = 0.1dec;
SELECT VALUE id FROM f WITH NOINDEX WHERE v >= 0.1f ORDER BY id;
SELECT VALUE id FROM f WHERE v >= 0.1f ORDER BY id;
SELECT VALUE id FROM f WITH NOINDEX WHERE v <= 0.1dec ORDER BY id;
SELECT VALUE id FROM f WHERE v <= 0.1dec ORDER BY id;
SELECT VALUE id FROM f WITH NOINDEX WHERE v = 0.5f ORDER BY id;
SELECT VALUE id FROM f WHERE v = 0.5f ORDER BY id;
SELECT VALUE id FROM f WHERE v IN [0.1dec, 0.5f] ORDER BY id;
SELECT VALUE id FROM f WITH NOINDEX WHERE v IN [0.1dec, 0.5f] ORDER BY id;

RETURN [array::len(array::distinct([0.1f, 0.1dec])), array::len(array::distinct([0.5f, 0.5dec]))];
RETURN { 0.1f, 0.1dec };
RETURN { 0.5f, 0.5dec };

EXPLAIN SELECT VALUE id FROM f WHERE v = 0.1f;
