/**
[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "Simplifying again after a record-idiom substitution is held to the same rules as the first pass: a denied call is only discarded where the runtime would never have evaluated it"

[[test.results]]
value = "[{ flag: 7, id: config:1 }]"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

*/

CREATE config:1 SET flag = 7;
CREATE t:1 SET a = 5;

-- The left operand collapses to `false`, and a runtime AND never evaluates its
-- right side once the left is false, so discarding the call matches evaluation.
SELECT * FROM t WHERE (config:1).flag IN [] AND math::max([1, 2]) > 0;

-- Reversed, the call is on the side the runtime does evaluate, so it stays.
SELECT * FROM t WHERE math::max([1, 2]) > 0 AND (config:1).flag IN [];

-- Substituting a computed field evaluates it, so the denial fires there too.
DEFINE FIELD computed_flag ON config COMPUTED math::max([1, 2]);
SELECT * FROM t WHERE (config:1).computed_flag IN [];
