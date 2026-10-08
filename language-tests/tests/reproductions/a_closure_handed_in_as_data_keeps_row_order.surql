/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A closure a statement is handed as data — a parameter, or a field of an
	object — and runs once per row, through a builtin that runs closures or as
	a method of the object, runs one row after another in the order read: its
	body cannot be seen where the statement is planned, so it may write or
	answer differently on each call. Sequence values come back in row order,
	and every write it makes lands.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[299, true]"

[[test.results]]
value = "299"

[[test.results]]
value = "[299, true]"

[[test.results]]
value = "299"

[[test.results]]
value = "299"

*/

DEFINE SEQUENCE sq START 1;
CREATE |t:1..300| RETURN NONE;
{ LET $f = |$x: any| sequence::nextval('sq'); LET $v = SELECT VALUE array::map([1], $f)[0] FROM t; RETURN [array::len($v), $v == array::sort($v)]; };
{ LET $g = |$x: any| { UPSERT stats:c SET n += 1; RETURN $x }; LET $w = SELECT VALUE array::map([1], $g) FROM t; RETURN SELECT VALUE n FROM ONLY stats:c; };
{ LET $obj = { inc: |$x: any| sequence::nextval('sq') }; LET $z = SELECT VALUE $obj.inc(1) FROM t; RETURN [array::len($z), $z == array::sort($z)]; };
{ LET $obj = { inc: |$x: any| { UPSERT stats:d SET n += 1; RETURN $x } }; LET $z = SELECT VALUE $obj.inc(1) FROM t; RETURN SELECT VALUE n FROM ONLY stats:d; };
{ LET $f = |$x: any| sequence::nextval('sq'); LET $v = SELECT VALUE array::filter([$x = 1], $f) FROM t; RETURN array::len($v); };
