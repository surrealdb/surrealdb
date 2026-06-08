/**
[env]
namespace = true
database = true

[test]
reason = "perf(planner): same-field eq-OR-eq chains fuse into LeafSetMembership; a = X OR a = Y must stay equivalent to a IN [X, Y] under loose equality, including cross-type numbers"
issue = 252

[[test.results]]
value = "[{ id: thing:a, v: 1 }]"

[[test.results]]
value = "[{ id: thing:b, v: 2 }]"

[[test.results]]
value = "[{ id: thing:c, v: 3 }]"

[[test.results]]
value = "[{ id: thing:d, v: 1f }]"

[[test.results]]
value = "[{ id: thing:e, v: 9 }]"

[[test.results]]
value = "[thing:a, thing:b, thing:d]"

[[test.results]]
value = "[thing:a, thing:b, thing:d]"

[[test.results]]
value = "[thing:a, thing:b, thing:c, thing:d]"

[[test.results]]
value = "[thing:a, thing:b, thing:c, thing:d]"

[[test.results]]
value = "true"

[[test.results]]
value = "[thing:a, thing:b, thing:d]"

[[test.results]]
value = "[thing:a, thing:c]"
*/

-- Table-scan data (no index), with a cross-type loose-equality row (thing:d has a float).
CREATE thing:a SET v = 1;
CREATE thing:b SET v = 2;
CREATE thing:c SET v = 3;
CREATE thing:d SET v = 1.0;
CREATE thing:e SET v = 9;

-- Two-arm OR fuses to set membership; must match the equivalent IN, including the float row.
SELECT VALUE id FROM thing WHERE v = 1 OR v = 2 ORDER BY id;
SELECT VALUE id FROM thing WHERE v IN [1, 2] ORDER BY id;

-- Three-arm OR chain fuses to a single set.
SELECT VALUE id FROM thing WHERE v = 1 OR v = 2 OR v = 3 ORDER BY id;
SELECT VALUE id FROM thing WHERE v IN [1, 2, 3] ORDER BY id;

-- OR / IN equivalence asserted directly.
RETURN (SELECT VALUE id FROM thing WHERE v = 1 OR v = 2 ORDER BY id)
     = (SELECT VALUE id FROM thing WHERE v IN [1, 2] ORDER BY id);

-- ExactEqual (==) is deliberately NOT fused at the planner level (the set path is
-- keyed on loose Value Eq); this query must still return correct results either way.
SELECT VALUE id FROM thing WHERE v == 1 OR v == 2 ORDER BY id;

-- The synthetic `id` field is derived from the KV key, which the set-membership
-- evaluator cannot read; `id = X OR id = Y` must NOT fuse and must stay correct.
-- (Regression guard for the GraphQL `id: { in: [...] }` filter, lowered to id-OR.)
SELECT VALUE id FROM thing WHERE id = thing:a OR id = thing:c ORDER BY id;
