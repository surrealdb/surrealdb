/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A pure subexpression is evaluated once within one evaluation region — a predicate with the projection beside it, a data clause, an IF condition chain, a FOR range — with the rows, the errors and the error order of evaluating each occurrence on its own. A cast that fails raises where its first occurrence is evaluated, with its message, and what it was to write is not written."

[[test.results]]
value = "[{ a: 3, id: t:1, v: 'x' }]"

[[test.results]]
value = "[{ a: 5, id: t:2, v: '7' }]"

[[test.results]]
value = "[{ x: 10 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ a: 3, id: t:1, v: 'x', x: 6, y: 7 }, { a: 5, id: t:2, v: '7', x: 10, y: 11 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[6, 10]"

[[test.results]]
value = "[{ a: 3, id: t:1, v: 'x', x: 6, y: 7 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'mid'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'small'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[6, 7]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[6, 7]"
*/

CREATE t:1 SET a = 3, v = 'x';
CREATE t:2 SET a = 5, v = '7';

-- A predicate and the projection beside it read one product.
SELECT a * 2 AS x FROM t WHERE a * 2 > 7;

-- The cast fails in the predicate on the first row, as it does unshared.
SELECT <int> v AS x FROM t WHERE <int> v > 0;

-- The values of a data clause are read from one snapshot.
UPDATE t SET x = a * 2, y = a * 2 + 1;

-- A cast that fails in a data clause writes nothing.
UPDATE t SET x = <int> v, y = <int> v + 1;
SELECT VALUE x FROM t;

-- A write's WHERE and its data clause are separate regions.
UPDATE t:1 SET x = a * 2, y = a * 2 + 1 WHERE a * 2 > 1;

-- An IF condition chain reads one product.
LET $p = 3;
IF $p * 2 > 10 { 'big' } ELSE IF $p * 2 > 1 { 'mid' } ELSE { 'small' };
LET $p = 0;
IF $p * 2 > 10 { 'big' } ELSE IF $p * 2 > 1 { 'mid' } ELSE { 'small' };

-- A condition that raises raises at its first occurrence.
LET $s = 'x';
IF <int> $s > 10 { 1 } ELSE IF <int> $s > 1 { 2 };

-- A FOR range reads one product, and a range whose element fails writes nothing.
LET $p = 3;
FOR $i IN [$p * 2, $p * 2 + 1] { CREATE r SET v = $i };
SELECT VALUE v FROM r ORDER BY v;
FOR $i IN [<int> $s, <int> $s + 1] { CREATE r SET v = $i };
SELECT VALUE v FROM r ORDER BY v;
