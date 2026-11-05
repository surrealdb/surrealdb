/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Events that are never fired by an action sit on either side of a live event that writes back
to its own table, and the chain spends the computation-depth budget level by level: the
statement fails with the depth error under the names of the events it passed through, and the
records it wrote roll back. The dormant events hold deeply nested clauses. A chain with a base
case, which the budget covers, runs to it, and a chain of events that never fire, longer than
the budget, runs. The failure here is that of the live event: a dormant event's own depth check
is not what ends the chain, and is pinned by a unit test of the check.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Error while processing event b_live: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: q0:1, x: 0 }]"

[[test.results]]
value = "[{ id: q0:1, x: 1 }]"

[[test.results]]
value = "[{ id: q0:1, x: 1 }]"
*/

DEFINE TABLE audit SCHEMALESS;
-- A live event that writes back to its own table, with dormant events on either side of it.
DEFINE TABLE spin SCHEMALESS;
DEFINE EVENT a_dormant_before ON spin WHEN false THEN { CREATE audit SET tag = 'before' };
DEFINE EVENT b_live ON spin WHEN $event = "CREATE" THEN { CREATE spin };
DEFINE EVENT c_dormant_after ON spin WHEN $event = "DELETE" THEN { CREATE audit SET tag = 'after' };
CREATE spin:start RETURN NONE;
SELECT count() FROM spin GROUP ALL;
SELECT count() FROM audit GROUP ALL;
-- The same chain, whose dormant events hold a deeply nested clause.
DEFINE TABLE spin_deep SCHEMALESS;
DEFINE EVENT a_dormant_deep ON spin_deep WHEN false THEN {
	CREATE audit SET v = (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE 1 FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x)
};
DEFINE EVENT b_live ON spin_deep WHEN $event = "CREATE" THEN { CREATE spin_deep };
DEFINE EVENT c_dormant_deep ON spin_deep WHEN $event = "DELETE" THEN {
	CREATE audit SET v = (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE (SELECT VALUE 1 FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x) FROM ONLY audit:x)
};
CREATE spin_deep:start RETURN NONE;
SELECT count() FROM spin_deep GROUP ALL;
-- A chain with a base case, which the budget covers.
DEFINE TABLE down SCHEMALESS;
DEFINE EVENT a_dormant ON down WHEN false THEN { CREATE audit SET tag = 'before' };
DEFINE EVENT b_live ON down WHEN $event = "CREATE" AND $after.n > 0 THEN { CREATE down SET n = $after.n - 1 };
DEFINE EVENT c_dormant ON down WHEN $event = "DELETE" THEN { CREATE audit SET tag = 'after' };
CREATE down:start SET n = 5 RETURN NONE;
SELECT VALUE n FROM down ORDER BY n;
-- A chain of distinct tables' events that never fire, longer than the budget allows.
DEFINE EVENT e ON q0 WHEN $event = "DELETE" THEN { CREATE q1 SET x = 1 };
DEFINE EVENT e ON q1 WHEN $event = "DELETE" THEN { CREATE q2 SET x = 1 };
DEFINE EVENT e ON q2 WHEN $event = "DELETE" THEN { CREATE q3 SET x = 1 };
DEFINE EVENT e ON q3 WHEN $event = "DELETE" THEN { CREATE q4 SET x = 1 };
DEFINE EVENT e ON q4 WHEN $event = "DELETE" THEN { CREATE q5 SET x = 1 };
DEFINE EVENT e ON q5 WHEN $event = "DELETE" THEN { CREATE q6 SET x = 1 };
DEFINE EVENT e ON q6 WHEN $event = "DELETE" THEN { CREATE q7 SET x = 1 };
DEFINE EVENT e ON q7 WHEN $event = "DELETE" THEN { CREATE q8 SET x = 1 };
DEFINE EVENT e ON q8 WHEN $event = "DELETE" THEN { CREATE q9 SET x = 1 };
DEFINE EVENT e ON q9 WHEN $event = "DELETE" THEN { CREATE q10 SET x = 1 };
DEFINE EVENT e ON q10 WHEN $event = "DELETE" THEN { CREATE q11 SET x = 1 };
DEFINE EVENT e ON q11 WHEN $event = "DELETE" THEN { CREATE q12 SET x = 1 };
DEFINE EVENT e ON q12 WHEN $event = "DELETE" THEN { CREATE q13 SET x = 1 };
DEFINE EVENT e ON q13 WHEN $event = "DELETE" THEN { CREATE q14 SET x = 1 };
DEFINE EVENT e ON q14 WHEN $event = "DELETE" THEN { CREATE q15 SET x = 1 };
DEFINE EVENT e ON q15 WHEN $event = "DELETE" THEN { CREATE q16 SET x = 1 };
DEFINE EVENT e ON q16 WHEN $event = "DELETE" THEN { CREATE q17 SET x = 1 };
DEFINE EVENT e ON q17 WHEN $event = "DELETE" THEN { CREATE q18 SET x = 1 };
DEFINE EVENT e ON q18 WHEN $event = "DELETE" THEN { CREATE q19 SET x = 1 };
DEFINE EVENT e ON q19 WHEN $event = "DELETE" THEN { CREATE q20 SET x = 1 };
DEFINE EVENT e ON q20 WHEN $event = "DELETE" THEN { CREATE q21 SET x = 1 };
DEFINE EVENT e ON q21 WHEN $event = "DELETE" THEN { CREATE q22 SET x = 1 };
DEFINE EVENT e ON q22 WHEN $event = "DELETE" THEN { CREATE q23 SET x = 1 };
DEFINE EVENT e ON q23 WHEN $event = "DELETE" THEN { CREATE q24 SET x = 1 };
DEFINE EVENT e ON q24 WHEN $event = "DELETE" THEN { CREATE q25 SET x = 1 };
DEFINE EVENT e ON q25 WHEN $event = "DELETE" THEN { CREATE q26 SET x = 1 };
DEFINE EVENT e ON q26 WHEN $event = "DELETE" THEN { CREATE q27 SET x = 1 };
DEFINE EVENT e ON q27 WHEN $event = "DELETE" THEN { CREATE q28 SET x = 1 };
DEFINE EVENT e ON q28 WHEN $event = "DELETE" THEN { CREATE q29 SET x = 1 };
DEFINE EVENT e ON q29 WHEN $event = "DELETE" THEN { CREATE q30 SET x = 1 };
DEFINE EVENT e ON q30 WHEN $event = "DELETE" THEN { CREATE q31 SET x = 1 };
CREATE q0:1 SET x = 0;
UPDATE q0:1 SET x = 1;
SELECT * FROM q0;
