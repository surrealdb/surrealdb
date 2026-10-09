/**
[env]
imports = ["language/expression/shared_subexpression_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A record user's reads are judged as they are unshared: a field a permission hides reads as NONE in every occurrence, so an expression over it raises where the first occurrence is evaluated; a computed field that draws a value reads one value within a row; a parameter the user may not read is refused at its first occurrence."

[[test.results]]
value = "[{ x: 6, y: 7 }, { x: 10, y: 11 }]"

[[test.results]]
error = "Cannot perform multiplication with 'none' and 'int'"

[[test.results]]
value = "[{ x: 10 }]"

[[test.results]]
error = "Cannot perform multiplication with 'none' and 'int'"

[[test.results]]
value = "[{ same: true }, { same: true }, { same: true }]"

[[test.results]]
error = "You don't have permission to view the $q parameter"
*/

-- The rows the permission admits.
SELECT a * 2 AS x, a * 2 + 1 AS y FROM t:1, t:3;
-- A row it hides reads as NONE, which a product refuses.
SELECT a * 2 AS x, a * 2 + 1 AS y FROM t;

SELECT a * 2 AS x FROM t:1, t:3 WHERE a * 2 > 7;
SELECT a * 2 AS x FROM t WHERE a * 2 > 1;

-- A computed field is one value within a row, however often it is read.
SELECT c + 0 = c + 0 AS same FROM t;

-- A parameter the user may not read.
SELECT $q * 2 AS x, $q * 2 AS y FROM ONLY t:1;
