/**
[test]
reason = "AND and OR yield one of their operands rather than a boolean, so a short-circuit rewrite may only replace the expression with a constant when the surviving operand could not have carried a value of its own"

[[test.results]]
value = "[{ flag: 7, id: config:1 }]"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "7"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "[]"

*/

CREATE config:1 SET flag = 7;
CREATE t:1 SET a = 5;

-- `7 OR true` is `7`, not `true`, so the comparison must still see 7.
RETURN 7 OR true;
RETURN 7 AND true;
RETURN NONE OR false;
RETURN NONE OR true;

-- The same value reaching the comparison through a fold, directly and via a
-- record-idiom substitution.
SELECT * FROM t WHERE (7 OR true) == 7;
SELECT * FROM t WHERE ((config:1).flag OR true) == 7;
SELECT * FROM t WHERE (7 AND true) == true;
SELECT * FROM t WHERE (NONE OR false) == false;

-- A boolean-valued operand still collapses, which is what the eliminating
-- folds rely on.
SELECT * FROM t WHERE (a > 0 OR true) == true;
SELECT * FROM t WHERE a IN [] AND a > 0;
