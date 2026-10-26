/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL concat and concat_ws skip NULL arguments, and concat_ws answers NULL for a NULL separator; a boolean joins as t or f"

[[test.results]]
value = "[{ a: 'a1', b: 'a-b', c: NULL, d: '' }]"

[[test.results]]
value = "[{ a: 'x,y', b: 'HELLO', c: NULL }]"

[[test.results]]
value = "[{ a: 'txf', b: 't,f' }]"
*/

SELECT concat('a', NULL, 1) AS a, concat_ws('-', 'a', NULL, 'b') AS b, concat_ws(NULL, 'a') AS c, concat(NULL) AS d;

SELECT concat_ws(',', 'x', 'y') AS a, upper('hello') AS b, lower(NULL) AS c;

SELECT concat(true, 'x', false) AS a, concat_ws(',', true, false) AS b;
