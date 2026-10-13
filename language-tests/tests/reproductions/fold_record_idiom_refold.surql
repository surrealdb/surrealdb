/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Substituting a row-independent record idiom leaves an operand constant that was not constant during the first fold, so the condition is simplified again and an eliminating comparison still reaches EmptyScan"

[[test.results]]
value = "[{ flag: 7, id: config:1 }]"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    EmptyScan [ctx: Rt]\n''''

*/

CREATE config:1 SET flag = 7;
CREATE t:1 SET a = 5;

-- `(config:1).flag` becomes `7` only after the record fold, and `7 INSIDE []`
-- is what collapses the scan.
EXPLAIN SELECT * FROM t WHERE (config:1).flag IN [];
