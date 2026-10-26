/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregate reads a group key or an aggregate's argument whose value fails
only about the shape of the data as `NONE`, so a statement that groups rows of
mixed shape goes on, but only for a value whose evaluation ran no nested
statement: a subquery, a block, or a `fn::` call that fails fails the grouped
query exactly as it fails the same query ungrouped, with the same error, and
leaves nothing written. Each shape is run ungrouped, under `GROUP ALL`, as a
`GROUP BY` key and as an aggregate's argument.
"""

[[test.results]]
value = "[{ id: s:1 }]"

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
error = "The table 'c' does not exist"

[[test.results]]
error = "Cannot perform addition with 'string' and 'int'"

[[test.results]]
error = "Cannot perform addition with 'string' and 'int'"

[[test.results]]
error = "Cannot perform addition with 'string' and 'int'"

[[test.results]]
error = "Cannot perform addition with 'string' and 'int'"

[[test.results]]
error = "The table 'i' does not exist"

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
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
error = "The table 'f' does not exist"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "[{ c: 1, t: 2 }]"

[[test.results]]
value = "[{ k: 1, t: 2 }, { k: 2, t: 0 }]"
*/

CREATE s:1;
DEFINE FUNCTION fn::writes_then_fails() { CREATE f:1; RETURN 1 + 'a'; };

-- A CREATE whose projection fails
SELECT (CREATE c:1 RETURN 1 + 'a') AS x FROM s;
SELECT (CREATE c:1 RETURN 1 + 'a') AS x FROM s GROUP ALL;
SELECT (CREATE c:1 RETURN 1 + 'a') AS x FROM s GROUP BY x;
SELECT array::group((CREATE c:1 RETURN 1 + 'a')) AS x FROM s GROUP ALL;
SELECT VALUE id FROM c;

-- A multi-row INSERT that fails on a later row
SELECT (INSERT INTO i [{ id: i:1, n: 1 }, { id: i:2, n: 'a' }] RETURN n + 1) AS x FROM s;
SELECT (INSERT INTO i [{ id: i:1, n: 1 }, { id: i:2, n: 'a' }] RETURN n + 1) AS x FROM s GROUP ALL;
SELECT (INSERT INTO i [{ id: i:1, n: 1 }, { id: i:2, n: 'a' }] RETURN n + 1) AS x FROM s GROUP BY x;
SELECT array::group((INSERT INTO i [{ id: i:1, n: 1 }, { id: i:2, n: 'a' }] RETURN n + 1)) AS x FROM s GROUP ALL;
SELECT VALUE id FROM i;

-- A read subquery that fails
SELECT (SELECT VALUE 1 + 'a' FROM ONLY s:1) AS x FROM s;
SELECT (SELECT VALUE 1 + 'a' FROM ONLY s:1) AS x FROM s GROUP ALL;
SELECT (SELECT VALUE 1 + 'a' FROM ONLY s:1) AS x FROM s GROUP BY x;
SELECT array::group((SELECT VALUE 1 + 'a' FROM ONLY s:1)) AS x FROM s GROUP ALL;

-- A block that writes, then fails
SELECT { CREATE b:1; 1 + 'a' } AS x FROM s;
SELECT { CREATE b:1; 1 + 'a' } AS x FROM s GROUP ALL;
SELECT { CREATE b:1; 1 + 'a' } AS x FROM s GROUP BY x;
SELECT array::group({ CREATE b:1; 1 + 'a' }) AS x FROM s GROUP ALL;
SELECT VALUE id FROM b;

-- A block that only computes
SELECT { 1 + 'a' } AS x FROM s;
SELECT { 1 + 'a' } AS x FROM s GROUP ALL;
SELECT { 1 + 'a' } AS x FROM s GROUP BY x;
SELECT array::group({ 1 + 'a' }) AS x FROM s GROUP ALL;

-- A function that writes, then fails
SELECT fn::writes_then_fails() AS x FROM s;
SELECT fn::writes_then_fails() AS x FROM s GROUP ALL;
SELECT fn::writes_then_fails() AS x FROM s GROUP BY x;
SELECT array::group(fn::writes_then_fails()) AS x FROM s GROUP ALL;
SELECT VALUE id FROM f;

-- A failing nested statement in the expression over a group's answers
SELECT math::sum(v) + (SELECT VALUE 1 + 'a' FROM ONLY s:1) AS x FROM [{ v: 1 }] GROUP ALL;

-- A value whose own operands do not add up still reads as `NONE`
SELECT count(v + 'x') AS c, math::sum(v + 1) AS t FROM [{ v: 1 }, { v: 'a' }] GROUP ALL;
SELECT k, math::sum(a + 1) AS t FROM [{ k: 1, a: 1 }, { k: 2, a: 'x' }] GROUP BY k ORDER BY k;
