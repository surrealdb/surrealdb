/**
[env]
planner-strategy = ["all-ro"]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "Refusing to fold a denied function keeps plan construction infallible, so EXPLAIN still returns a plan with the call left in the predicate"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'a > math::max(...)', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

*/

CREATE t:1 SET a = 5;

-- EXPLAIN plans without executing. The denied call is neither folded nor
-- raised as an error during planning; it survives in the predicate and would
-- only fail if the plan were run.
SELECT * FROM t WHERE a > math::max([1, 2]) EXPLAIN;
