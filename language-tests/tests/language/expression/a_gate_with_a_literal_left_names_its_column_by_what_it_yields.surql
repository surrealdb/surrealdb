/**
[env]

[test]
reason = "An AND or OR whose left operand is a boolean literal is its left literal or its right operand, so an unaliased column over it is named by that value, whichever operand it is, while a gate whose right operand is the literal keeps the written expression as its key"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ p: 5 }]"

[[test.results]]
value = "[{ p: 5 }]"

[[test.results]]
value = "[{ a: 1 }]"

[[test.results]]
value = "[{ a: 1, b: 2 }]"

[[test.results]]
value = "[{ p: 5 }]"

[[test.results]]
value = "[{ p: 5 }]"

[[test.results]]
value = "[{ false: false }]"

[[test.results]]
value = "[{ true: true }]"

[[test.results]]
value = '[{ "a AND b": 2 }]'

[[test.results]]
value = '[{ "1 + a": 2 }]'

[[test.results]]
value = '[{ "a > 1": false }]'

[[test.results]]
value = '[{ "a AND true": true }]'

[[test.results]]
value = '[{ "a OR false": 1 }]'

[[test.results]]
value = '[{ "a AND false": false }]'

[[test.results]]
value = '[{ "a OR true": 1 }]'

[[test.results]]
value = "[{ a: 1, count: 1, p: 5 }]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 1, b = 2;
LET $p = 5;
SELECT true AND $p FROM t;
SELECT false OR $p FROM t;
SELECT true AND a FROM t;
SELECT false OR b, a FROM t;
SELECT true && $p FROM t;
SELECT true AND true AND $p FROM t;
SELECT false AND a FROM t;
SELECT true OR a FROM t;
SELECT (true AND a) AND b FROM t;
SELECT 1 + (true AND a) FROM t;
SELECT true AND a > 1 FROM t;
SELECT a AND true FROM t;
SELECT a OR false FROM t;
SELECT a AND false FROM t;
SELECT a OR true FROM t;
SELECT a, true AND $p, count() FROM t GROUP BY a;
