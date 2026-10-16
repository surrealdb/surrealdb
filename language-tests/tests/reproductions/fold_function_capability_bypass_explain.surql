/**
[env]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "Refusing to fold a denied function keeps plan construction infallible, so EXPLAIN still returns a plan with the call left in the predicate; an unreachable operand still collapses to EmptyScan"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: a > math::max([1, 2])]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [once: math::floor(1.5f) INSIDE []]
        Union [ctx: Db]
''''
*/

CREATE t:1 SET a = 5;

-- EXPLAIN plans without executing. The denied call is neither folded nor
-- raised as an error during planning; it survives in the predicate and would
-- only fail if the plan were run.
EXPLAIN SELECT * FROM t WHERE a > math::max([1, 2]);

-- `false AND x` never reaches `x` at runtime, so the collapse still happens
-- and the plan is still EmptyScan rather than a full scan carrying a
-- predicate that can admit no row.
EXPLAIN SELECT * FROM t WHERE false AND math::max([1, 2]) > 0;

-- With a restriction in force the collapse is refused for any call, because
-- an allowed call can reach a denied one through a body invisible at plan
-- time. `math::floor(1.5)` is unaffected: folding is bottom-up, so it is
-- already a literal by the time the collapse is considered.
EXPLAIN SELECT * FROM t WHERE math::floor(1.5) IN [];
