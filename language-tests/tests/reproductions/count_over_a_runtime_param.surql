/**
[env]
namespace = true
database = true

[test]
reason = """
A block-local `LET` is not substituted at plan time, so
`SELECT count() FROM $x GROUP ALL` reaches the count fast path as an
unresolved parameter. `CountScan` counts keys and has nothing to count for an
array, a string or an object, so a source the planner cannot prove names a key
range stays on the full pipeline, which counts whatever rows the value yields.
"""

[[test.results]]
value = "[{ id: person:1 }, { id: person:2 }, { id: person:3 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"
*/

CREATE person:1, person:2, person:3;

{ LET $x = [1, 2]; RETURN SELECT count() FROM $x GROUP ALL; };
{ LET $z = "hello"; RETURN SELECT count() FROM $z GROUP ALL; };
{ LET $o = { a: 1 }; RETURN SELECT count() FROM $o GROUP ALL; };

-- A parameter that does name a record id is answered the same way it always
-- was; the fast path is not what makes it work.
{ LET $i = person:1; RETURN SELECT count() FROM $i GROUP ALL; };

-- The sources the planner can prove keep the key-range count.
SELECT count() FROM person GROUP ALL;
SELECT count() FROM person:1..3 GROUP ALL;
