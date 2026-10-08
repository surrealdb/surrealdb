/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A `SELECT` whose projection calls a user-defined function that writes runs
	its rows one after another, in the order read, however many rows it
	reads: no two calls stage writes against the transaction at once.

	Each call of `fn::inc` adds one to a counter, so the counter ends at the
	number of rows. Each call of `fn::bump` updates one indexed record, so the
	index ends holding the one entry for the record's last value: the read
	through the index and the read past it agree.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:1, v: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[299, true]"

[[test.results]]
value = "299"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:1, v: 299 }]"

[[test.results]]
value = "[{ id: r:1, v: 299 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ id: r:1, v: 299 }]"

[[test.results]]
value = "[]"

*/

DEFINE FUNCTION fn::inc($x: any) { UPSERT stats:views SET n += 1; RETURN $x; };
DEFINE TABLE r SCHEMALESS;
DEFINE INDEX r_v ON r FIELDS v;
CREATE r:1 SET v = 0;
DEFINE FUNCTION fn::bump($x: any) { UPDATE r:1 SET v = record::id($x); RETURN $x; };
CREATE |t:1..300| RETURN NONE;
LET $all = SELECT VALUE fn::inc(id) FROM t;
RETURN [array::len($all), $all == (SELECT VALUE id FROM t)];
SELECT VALUE n FROM ONLY stats:views;
LET $bumped = SELECT VALUE fn::bump(id) FROM t;
SELECT * FROM r WITH INDEX r_v WHERE v >= 0;
SELECT * FROM r WITH NOINDEX WHERE v >= 0;
SELECT count() FROM r WITH INDEX r_v WHERE v >= 0 GROUP ALL;
SELECT * FROM r WITH INDEX r_v WHERE v = 299;
SELECT * FROM r WITH INDEX r_v WHERE v = 500;
