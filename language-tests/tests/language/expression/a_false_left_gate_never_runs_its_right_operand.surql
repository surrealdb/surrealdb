/**
[env]

[test]
reason = "false AND x is false and true OR x is true whether or not x can raise, because the gate evaluates left to right and never runs x; x AND false and x OR true run x first, so they raise when x does"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `1`"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `1`"

[[test.results]]
value = "[{ false: false }]"

[[test.results]]
value = "[{ true: true }]"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `1`"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `1`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `1`"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `1`"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 1;
RETURN false AND string::len(1);
RETURN true OR string::len(1);
RETURN string::len(1) AND false;
RETURN string::len(1) OR true;
SELECT false AND string::len(a) FROM t;
SELECT true OR string::len(a) FROM t;
SELECT string::len(a) AND false FROM t;
SELECT string::len(a) OR true FROM t;
SELECT * FROM t WHERE false AND string::len(a);
SELECT * FROM t WHERE string::len(a) AND false;
SELECT * FROM t WHERE string::len(a) OR true;
