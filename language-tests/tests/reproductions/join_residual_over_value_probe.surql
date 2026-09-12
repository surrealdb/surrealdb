/**
[test]
reason = """
Decorrelating `x IN (SELECT VALUE …)` into a join judges the subquery's
remaining correlated conjuncts as a residual, read off the joined row. A
probe row that is not an object -- here a record id, from a `VALUE`
projection -- has no field to carry the join's alias, so the residual would
be judged against an object holding the alias alone. Every other name answers
NONE there, but off a record id those names do not answer NONE: a field read
fetches the record. `$parent.foo` would read NONE instead of 50, `10 >= NONE`
holds where `10 >= 50` does not, and the row would be returned rather than
rejected.

The second query is the same predicate over an object probe row, where the
alias travels on the row and the residual reads the outer fields it names.
The third has no residual to judge, so the lift stands whatever the probe
rows are.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ foo: 50, id: outr:1, k: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: rgt:1, k: 'a', n: 10 }]"

# Record-id probe rows, correlated residual: 10 >= 50 is false.
[[test.results]]
value = "[]"

# Object probe rows, same predicate.
[[test.results]]
value = "[]"

# No residual: the lift stands.
[[test.results]]
value = "[outr:1]"

*/

DEFINE TABLE outr SCHEMALESS;
CREATE outr:1 SET foo = 50, k = 'a';
DEFINE TABLE rgt SCHEMALESS;
CREATE rgt:1 SET k = 'a', n = 10;
SELECT VALUE id FROM (SELECT VALUE id FROM outr) WHERE k IN (SELECT VALUE k FROM rgt WHERE n >= $parent.foo);
SELECT VALUE id FROM outr WHERE k IN (SELECT VALUE k FROM rgt WHERE n >= $parent.foo);
SELECT VALUE id FROM outr WHERE k IN (SELECT VALUE k FROM rgt);
