/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "A fold that eliminates a subtree must classify every FROM source, not just reject any list that is not all tables: a scalar source dereferences nothing and keeps the table side collapsing to EmptyScan, while an array or parameter source can yield a record id and must hold the condition"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Filter [ctx: Db] [predicate: false]\n        Union [ctx: Db] [inputs: 2]\n            EmptyScan [ctx: Rt]\n            SourceExpr [ctx: Db] [expr: 1]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Filter [ctx: Db] [predicate: false]\n        Union [ctx: Db] [inputs: 2]\n            EmptyScan [ctx: Rt]\n            SourceExpr [ctx: Db] [expr: { b: 1 }]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Filter [ctx: Db] [predicate: false]\n        Union [ctx: Db] [inputs: 2]\n            EmptyScan [ctx: Rt]\n            SourceExpr [ctx: Db] [expr: 3.141592653589793f]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Filter [ctx: Db] [predicate: a INSIDE []]\n        Union [ctx: Db] [inputs: 2]\n            TableScan [ctx: Db] [table: t, direction: Forward]\n            SourceExpr [ctx: Db] [expr: [1, 2]]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Filter [ctx: Db] [predicate: a INSIDE []]\n        Union [ctx: Db] [inputs: 2]\n            TableScan [ctx: Db] [table: t, direction: Forward]\n            SourceExpr [ctx: Db] [expr: $p]\n''''

*/

CREATE t:1 SET a = 5;

-- A scalar source yields that scalar, and a field read off a scalar is NONE,
-- so the condition still folds and the table side selects EmptyScan.
EXPLAIN SELECT * FROM t, 1 WHERE a IN [];

-- An object row's bare field read is a key lookup, same as a record's.
EXPLAIN SELECT * FROM t, { b: 1 } WHERE a IN [];

-- A constant yields a float, datetime or duration, so it is a scalar row too.
EXPLAIN SELECT * FROM t, math::PI WHERE a IN [];

-- An array or parameter source can yield a record id, where the same read
-- dereferences it and runs computed fields and permissions, so the condition
-- has to survive planning and the table is scanned.
EXPLAIN SELECT * FROM t, [1, 2] WHERE a IN [];
EXPLAIN SELECT * FROM t, $p WHERE a IN [];
