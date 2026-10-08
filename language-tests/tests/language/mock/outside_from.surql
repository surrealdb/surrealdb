/**
[test]
reason = """
A mock record set is an expression, so it can be written anywhere an
expression can: returned, bound by `LET`, passed to a function, indexed, and
iterated by `FOR`. Outside `FROM` it evaluates to the array of record ids it
names. A ranged mock names fixed ids; a counted mock names generated ones, so
only its length is asserted.
"""

[[test.results]]
value = "[t:2, t:3, t:4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:2, t:3, t:4]"

[[test.results]]
value = "5"

[[test.results]]
value = "t:3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:2 }, { id: t:3 }]"
*/

RETURN |t:2..5|;
LET $m = |t:2..5|;
RETURN $m;
RETURN array::len(|t:5|);
RETURN |t:2..5|[1];
FOR $id IN |t:2..4| { CREATE $id SET n = 1 };
SELECT id FROM t ORDER BY id;
