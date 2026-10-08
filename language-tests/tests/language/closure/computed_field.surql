/**
[env]
namespace = true
database = true

[test]
reason = """
A closure a computed field invokes runs while the field is being computed:
one the field's expression makes, one it hands a builtin, one a function
returns, and one whose body reads the record being computed, which reads the
record's stored fields rather than computing the field again.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: thing:1, n: 1 }]"

[[test.results]]
value = "[{ id: thing:2, n: 2 }]"

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
value = "[{ c: 2, d: [2, 3], e: [3], f: 100, g: 1, id: thing:1, n: 1 }, { c: 4, d: [4, 5], e: [6], f: 200, g: 2, id: thing:2, n: 2 }]"

[[test.results]]
value = "[[2, [2, 3], [3], 100, 1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 4]"
*/

DEFINE TABLE thing SCHEMALESS;
DEFINE FUNCTION fn::mk() { RETURN |$x| $x * 3 };
CREATE thing:1 SET n = 1;
CREATE thing:2 SET n = 2;
DEFINE FIELD c ON thing COMPUTED (|$x| $x * 2)(n);
DEFINE FIELD d ON thing COMPUTED array::map([n, n + 1], |$v| $v + n);
DEFINE FIELD e ON thing COMPUTED array::map([n], fn::mk());
DEFINE FIELD f ON thing COMPUTED (|$r| $r.n * 100)(id);
DEFINE FIELD g ON thing COMPUTED (|$r| (SELECT VALUE n FROM ONLY $r))(id);
SELECT * FROM thing ORDER BY id;
SELECT VALUE [c, d, e, f, g] FROM thing:1;
LET $h = |$r| $r.c;
RETURN array::map([thing:1, thing:2], $h);
