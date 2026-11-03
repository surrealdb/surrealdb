/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregate reads a group key or an aggregate's argument as `NONE` when its
value fails only about the shape of the data, and only when evaluating it ran
no nested statement: no subquery, block, `fn::` call, embedded script or
builtin that runs a statement, however deep it ran (through a closure, a
computed field or a graph lookup) and whether the failure came out of it or
after it. Such a value fails the grouped query with the error the same query
gives ungrouped, and what the nested statement wrote is undone with the
query. A value computed by a closure alone still reads as `NONE`, as does
one computed by a `fn::` body small and pure enough for the plan to run in the
caller, which is then an expression of the caller's, as a closure's body is.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "The table 't' does not exist"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "The table 'w' does not exist"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "The table 'b' does not exist"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "The table 'z' does not exist"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "[{ x: [NONE] }]"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "[{ x: [NONE] }]"

[[test.results]]
value = "[{ x: [NONE] }]"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "The table 'y' does not exist"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "The table 'y2' does not exist"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "The table 'y3' does not exist"
*/

CREATE s:1 RETURN NONE;
CREATE r:1 RETURN NONE;
RELATE s:1->e->v:1 RETURN NONE;
DEFINE FIELD g ON r COMPUTED (SELECT VALUE 1 + 'a' FROM ONLY s:1);
DEFINE FUNCTION fn::adds($a: any) { LET $b = $a + 'a'; RETURN $b; };

-- A writing subquery in a closure the expression calls
SELECT (|| (CREATE t:1 RETURN 1 + 'a'))() AS x FROM s;
SELECT (|| (CREATE t:1 RETURN 1 + 'a'))() AS x FROM s GROUP ALL;
SELECT (|| (CREATE t:1 RETURN 1 + 'a'))() AS x FROM s GROUP BY x;
SELECT array::group((|| (CREATE t:1 RETURN 1 + 'a'))()) AS x FROM s GROUP ALL;

-- A writing subquery in a closure a method calls
SELECT [1].map(|$x| (CREATE t:2 RETURN 1 + 'a')) AS x FROM s;
SELECT [1].map(|$x| (CREATE t:2 RETURN 1 + 'a')) AS x FROM s GROUP ALL;
SELECT [1].map(|$x| (CREATE t:2 RETURN 1 + 'a')) AS x FROM s GROUP BY x;
SELECT array::group([1].map(|$x| (CREATE t:2 RETURN 1 + 'a'))) AS x FROM s GROUP ALL;
SELECT VALUE id FROM t;

-- A computed field whose subquery fails
SELECT r:1.g AS x FROM s;
SELECT r:1.g AS x FROM s GROUP ALL;
SELECT r:1.g AS x FROM s GROUP BY x;
SELECT array::group(r:1.g) AS x FROM s GROUP ALL;

-- A writing subquery in a graph lookup's condition
SELECT ->e[WHERE (CREATE w:1 RETURN 1 + 'a')]->v AS x FROM s;
SELECT ->e[WHERE (CREATE w:1 RETURN 1 + 'a')]->v AS x FROM s GROUP ALL;
SELECT ->e[WHERE (CREATE w:1 RETURN 1 + 'a')]->v AS x FROM s GROUP BY x;
SELECT array::group(->e[WHERE (CREATE w:1 RETURN 1 + 'a')]->v) AS x FROM s GROUP ALL;
SELECT VALUE id FROM w;

-- A block that only returns a value
SELECT { RETURN 1 + 'a'; } AS x FROM s;
SELECT { RETURN 1 + 'a'; } AS x FROM s GROUP ALL;
SELECT { RETURN 1 + 'a'; } AS x FROM s GROUP BY x;
SELECT array::group({ RETURN 1 + 'a'; }) AS x FROM s GROUP ALL;

-- A block that returns a value its subquery wrote
SELECT { RETURN (CREATE b:1 RETURN VALUE 1)[0] + 'a'; } AS x FROM s;
SELECT { RETURN (CREATE b:1 RETURN VALUE 1)[0] + 'a'; } AS x FROM s GROUP ALL;
SELECT { RETURN (CREATE b:1 RETURN VALUE 1)[0] + 'a'; } AS x FROM s GROUP BY x;
SELECT array::group({ RETURN (CREATE b:1 RETURN VALUE 1)[0] + 'a'; }) AS x FROM s GROUP ALL;
SELECT VALUE id FROM b;

-- A subquery that wrote, then a value computed from it that fails
SELECT (CREATE z:1 RETURN VALUE 1)[0] + 'a' AS x FROM s;
SELECT (CREATE z:1 RETURN VALUE 1)[0] + 'a' AS x FROM s GROUP ALL;
SELECT (CREATE z:1 RETURN VALUE 1)[0] + 'a' AS x FROM s GROUP BY x;
SELECT array::group((CREATE z:1 RETURN VALUE 1)[0] + 'a') AS x FROM s GROUP ALL;
SELECT VALUE id FROM z;

-- A function whose body only computes, run as a call
SELECT fn::adds(1) AS x FROM s;
SELECT fn::adds(1) AS x FROM s GROUP ALL;
SELECT fn::adds(1) AS x FROM s GROUP BY x;
SELECT array::group(fn::adds(1)) AS x FROM s GROUP ALL;

-- A block in a closure a method calls
SELECT [1].map(|$x| { RETURN $x + 'a'; }) AS x FROM s;
SELECT array::group([1].map(|$x| { RETURN $x + 'a'; })) AS x FROM s GROUP ALL;

-- A closure alone still reads as `NONE` grouped
SELECT [1].map(|$x| $x + 'a') AS x FROM s;
SELECT array::group([1].map(|$x| $x + 'a')) AS x FROM s GROUP ALL;

-- A block that succeeds, then the expression around it fails
SELECT { LET $v = 1; $v } + 'a' AS x FROM s;
SELECT { LET $v = 1; $v } + 'a' AS x FROM s GROUP ALL;
SELECT { LET $v = 1; $v } + 'a' AS x FROM s GROUP BY x;
SELECT array::group({ LET $v = 1; $v } + 'a') AS x FROM s GROUP ALL;

-- The same in a closure a method calls
SELECT [1].map(|$x| { LET $v = $x; $v } + 'a') AS x FROM s;
SELECT [1].map(|$x| { LET $v = $x; $v } + 'a') AS x FROM s GROUP ALL;
SELECT [1].map(|$x| { LET $v = $x; $v } + 'a') AS x FROM s GROUP BY x;
SELECT array::group([1].map(|$x| { LET $v = $x; $v } + 'a')) AS x FROM s GROUP ALL;

-- The same in the body of a closure the expression calls
SELECT (|| { RETURN 1; })() + 'a' AS x FROM s;
SELECT (|| { RETURN 1; })() + 'a' AS x FROM s GROUP ALL;
SELECT (|| { RETURN 1; })() + 'a' AS x FROM s GROUP BY x;
SELECT array::group((|| { RETURN 1; })() + 'a') AS x FROM s GROUP ALL;

-- The same in a computed field
CREATE r3:1 RETURN NONE;
DEFINE FIELD h ON r3 COMPUTED { RETURN 1; };
SELECT r3:1.h + 'a' AS x FROM s;
SELECT r3:1.h + 'a' AS x FROM s GROUP ALL;
SELECT r3:1.h + 'a' AS x FROM s GROUP BY x;
SELECT array::group(r3:1.h + 'a') AS x FROM s GROUP ALL;

-- A closure alone, and a value that fails before a block is entered, still read as `NONE`
SELECT (|| 1)() + 'a' AS x FROM s GROUP ALL;
SELECT array::group('a' + 1 + { RETURN 1; }) AS x FROM s GROUP ALL;

-- An analysed statement that writes, then fails inside the block that holds it
SELECT { EXPLAIN ANALYZE CREATE y:1 RETURN 1 + 'a' } AS x FROM s;
SELECT { EXPLAIN ANALYZE CREATE y:1 RETURN 1 + 'a' } AS x FROM s GROUP ALL;
SELECT { EXPLAIN ANALYZE CREATE y:1 RETURN 1 + 'a' } AS x FROM s GROUP BY x;
SELECT array::group({ EXPLAIN ANALYZE CREATE y:1 RETURN 1 + 'a' }) AS x FROM s GROUP ALL;
SELECT VALUE id FROM y;

-- The same as a subquery
SELECT (EXPLAIN ANALYZE CREATE y2:1 RETURN 1 + 'a') AS x FROM s;
SELECT (EXPLAIN ANALYZE CREATE y2:1 RETURN 1 + 'a') AS x FROM s GROUP ALL;
SELECT (EXPLAIN ANALYZE CREATE y2:1 RETURN 1 + 'a') AS x FROM s GROUP BY x;
SELECT array::group((EXPLAIN ANALYZE CREATE y2:1 RETURN 1 + 'a')) AS x FROM s GROUP ALL;
SELECT VALUE id FROM y2;

-- An analysed statement that wrote, then a value computed after it fails
SELECT { EXPLAIN ANALYZE CREATE y3:1 RETURN VALUE 1; RETURN 1 + 'a'; } AS x FROM s;
SELECT { EXPLAIN ANALYZE CREATE y3:1 RETURN VALUE 1; RETURN 1 + 'a'; } AS x FROM s GROUP ALL;
SELECT { EXPLAIN ANALYZE CREATE y3:1 RETURN VALUE 1; RETURN 1 + 'a'; } AS x FROM s GROUP BY x;
SELECT array::group({ EXPLAIN ANALYZE CREATE y3:1 RETURN VALUE 1; RETURN 1 + 'a'; }) AS x FROM s GROUP ALL;
SELECT VALUE id FROM y3;
