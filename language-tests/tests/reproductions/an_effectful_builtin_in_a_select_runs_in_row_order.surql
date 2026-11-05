/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A `SELECT` whose projection calls a builtin declared to answer differently
	on each call runs its rows one after another, in the order read, however
	many rows it reads: called directly, through a user-defined function that
	only reads, or under a `WHERE`.

	`sequence::nextval` advances a counter, so in row order its values rise
	with the rows. `rand::uuid::v7` is time-ordered, so in row order its values
	sort as they were drawn.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[299, true, 1, 299]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[299, true, 1, 299]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[149, true]"

*/

DEFINE SEQUENCE sq START 1;
DEFINE SEQUENCE sq2 START 1;
DEFINE FUNCTION fn::next() { RETURN sequence::nextval('sq2'); };
CREATE |t:1..300| RETURN NONE;
LET $v = SELECT VALUE sequence::nextval('sq') FROM t;
RETURN [array::len($v), $v == array::sort($v), array::first($v), array::last($v)];
LET $w = SELECT VALUE fn::next() FROM t;
RETURN [array::len($w), $w == array::sort($w), array::first($w), array::last($w)];
LET $u = SELECT VALUE rand::uuid::v7() FROM t;
RETURN $u == array::sort($u);
LET $n = SELECT VALUE { s: sequence::nextval('sq'), id: id } FROM t WHERE record::id(id) % 2 = 0;
RETURN [array::len($n), $n.s == array::sort($n.s)];
