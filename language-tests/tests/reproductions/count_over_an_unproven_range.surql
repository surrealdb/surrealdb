/**
[env]
namespace = true
database = true

[test]
reason = """
`CountScan` counts keys, so the count fast path may take only a source the
planner can prove names a key range. A postfix expression is not that proof on
its own: `..` and `>..` name a key range only over a record id written down as
one, and a postfix call or method call answers with whatever its body returns.

Each source below yields rows the full pipeline counts and a key count cannot,
and each reached `CountScan`, which raised on every value that was neither a
table nor a record id.
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
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 3 }]"
*/

CREATE person:1, person:2, person:3;

-- A closure invocation returns whatever its body holds.
SELECT count() FROM (|| [1, 2])() GROUP ALL;

-- An open range over something that is not a record id is a range of numbers,
-- or of whatever the parameter holds. The parentheses group the range against
-- the clause that follows it; the source the planner sees is the range itself.
SELECT count() FROM (5..) GROUP ALL;
{ LET $p = person:1; RETURN SELECT count() FROM ($p..) GROUP ALL; };

-- The ranges that are written down as record ids keep the key count.
SELECT count() FROM person:1.. GROUP ALL;
SELECT count() FROM person:1>.. GROUP ALL;
SELECT count() FROM person:1..3 GROUP ALL;
SELECT count() FROM person GROUP ALL;
