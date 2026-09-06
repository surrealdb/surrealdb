/**
[env]
planner-strategy = ["all-ro"]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "Refusing to fold a denied function keeps plan construction infallible, so EXPLAIN still returns a plan with the call left in the predicate; an unreachable operand still collapses to EmptyScan"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'a > math::max(...)', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ context: 'Rt', operator: 'EmptyScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ context: 'Rt', operator: 'EmptyScan' }], context: 'Db', operator: 'SelectProject' }"

*/

CREATE t:1 SET a = 5;

-- EXPLAIN plans without executing. The denied call is neither folded nor
-- raised as an error during planning; it survives in the predicate and would
-- only fail if the plan were run.
SELECT * FROM t WHERE a > math::max([1, 2]) EXPLAIN;

-- `false AND x` never reaches `x` at runtime, so the collapse still happens
-- and the plan is still EmptyScan rather than a full scan carrying a
-- predicate that can admit no row.
SELECT * FROM t WHERE false AND math::max([1, 2]) > 0 EXPLAIN;

-- With a restriction in force the collapse is refused for any call, because
-- an allowed call can reach a denied one through a body invisible at plan
-- time. `math::floor(1.5)` is unaffected: folding is bottom-up, so it is
-- already a literal by the time the collapse is considered.
SELECT * FROM t WHERE math::floor(1.5) IN [] EXPLAIN;
