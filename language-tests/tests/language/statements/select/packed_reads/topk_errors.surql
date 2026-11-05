/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "A fallible filter above a top-k scan of a SCHEMAFULL table read in batches raises the first error of the first row it fails on, naming the same row, and a filter that never reaches the failing row answers as it would without one."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `int` using input `'boom'`"

[[test.results]]
error = "Could not cast into `int` using input `'boom'`"

[[test.results]]
error = "An error occurred: bad boom"

[[test.results]]
error = "Could not cast into `int` using input `'boom'`"

[[test.results]]
value = "[{ id: e:0 }, { id: e:1 }, { id: e:2 }, { id: e:3 }, { id: e:4 }]"

[[test.results]]
value = "[{ id: e:1099 }, { id: e:1098 }, { id: e:1097 }, { id: e:1096 }, { id: e:1095 }]"

[[test.results]]
value = "[{ a: 0, id: e:0 }, { a: 1, id: e:1 }, { a: 2, id: e:2 }]"

[[test.results]]
error = "Could not cast into `int` using input `'boom'`"

[[test.results]]
value = "[{ id: d:0 }, { id: d:1 }, { id: d:2 }, { id: d:3 }, { id: d:4 }]"

[[test.results]]
value = "[{ id: d:1099 }, { id: d:1098 }, { id: d:1097 }, { id: d:1096 }, { id: d:1095 }]"

[[test.results]]
value = "[{ a: 0, id: d:0 }, { a: 1, id: d:1 }, { a: 2, id: d:2 }]"

*/

DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
DEFINE FIELD s ON e TYPE string;
FOR $n IN 0..1200 { CREATE type::record('e', $n) CONTENT { a: $n, s: '7' } RETURN NONE; };
UPDATE e:1100 SET s = 'boom' RETURN NONE;
UPDATE e:1150 SET s = 'worse' RETURN NONE;
DEFINE TABLE d SCHEMALESS;
FOR $n IN 0..1200 { CREATE type::record('d', $n) CONTENT { a: $n, s: '7' } RETURN NONE; };
UPDATE d:1100 SET s = 'boom' RETURN NONE;
UPDATE d:1150 SET s = 'worse' RETURN NONE;
DEFINE FUNCTION fn::chk($x: string) { IF $x = 'boom' OR $x = 'worse' { THROW 'bad ' + $x; }; RETURN true; };
SELECT id FROM e WHERE type::int(s) > 1 ORDER BY a LIMIT 5;
SELECT id FROM e WHERE type::int(s) > 1 ORDER BY a LIMIT 5 START 2;
SELECT id FROM e WHERE fn::chk(s) ORDER BY a LIMIT 5;
SELECT id FROM e WHERE a > 3 AND type::int(s) > 1 ORDER BY a DESC LIMIT 5;
SELECT id FROM e WHERE a < 1100 AND type::int(s) > 1 ORDER BY a LIMIT 5;
SELECT id FROM e WHERE a < 1100 AND fn::chk(s) ORDER BY a DESC LIMIT 5;
SELECT id, a FROM e WHERE a < 1100 ORDER BY a LIMIT 3;
SELECT id FROM d WHERE a > 3 AND type::int(s) > 1 ORDER BY a DESC LIMIT 5;
SELECT id FROM d WHERE a < 1100 AND type::int(s) > 1 ORDER BY a LIMIT 5;
SELECT id FROM d WHERE a < 1100 AND fn::chk(s) ORDER BY a DESC LIMIT 5;
SELECT id, a FROM d WHERE a < 1100 ORDER BY a LIMIT 3;
