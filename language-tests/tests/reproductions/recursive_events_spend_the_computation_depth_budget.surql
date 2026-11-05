/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Events whose clauses write records whose events fire again spend the
	computation-depth budget level by level: each write spends what its
	statement spends, and each clause what it spends on the way to its write.
	Once the budget is spent the statement fails with the depth error, under
	the name of every event it passed through, and its writes roll back,
	rather than recursing without bound. Recursion that stops sooner, and a
	chain of events too long for the budget whose events never fire, run.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: child:1, parent: parent:1 }]"

[[test.results]]
error = "Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[{ id: child:1, n: 0, parent: parent:1 }]"

[[test.results]]
value = "[{ id: child:1, n: 0, parent: parent:1 }]"

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
error = "Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[]"

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

*/

-- An event that creates a record of its own table, in a block and bare.
DEFINE EVENT e ON t WHEN $event = 'CREATE' THEN { CREATE t SET x = $after.x + 1 };
CREATE t SET x = 0;
SELECT count() FROM t GROUP ALL;
DEFINE EVENT e ON u WHEN $event = 'CREATE' THEN (CREATE u SET x = $after.x + 1);
CREATE u SET x = 0;
SELECT count() FROM u GROUP ALL;
-- Two tables whose events create each other's records.
DEFINE EVENT e ON a THEN { CREATE b SET x = 1 };
DEFINE EVENT e ON b THEN { CREATE a SET x = 1 };
CREATE a SET x = 0;
SELECT count() FROM a GROUP ALL;
-- Two records whose events update each other.
DEFINE EVENT e ON child THEN { UPDATE parent SET n += 1 WHERE id = $after.parent };
DEFINE EVENT e ON parent THEN { UPDATE child SET n += 1 };
CREATE child:1 SET parent = parent:1;
CREATE parent:1;
UPDATE child:1 SET n = 0;
SELECT * FROM child, parent;
-- Recursion that stops before the budget is spent.
DEFINE EVENT e ON s WHEN $after.x < 5 THEN (CREATE s SET x = $after.x + 1);
CREATE s SET x = 0 RETURN NONE;
SELECT VALUE x FROM s ORDER BY x;
-- A chain of distinct tables' events, longer than the budget allows.
DEFINE EVENT e ON c0 THEN { CREATE c1 SET x = 1 };
DEFINE EVENT e ON c1 THEN { CREATE c2 SET x = 1 };
DEFINE EVENT e ON c2 THEN { CREATE c3 SET x = 1 };
DEFINE EVENT e ON c3 THEN { CREATE c4 SET x = 1 };
DEFINE EVENT e ON c4 THEN { CREATE c5 SET x = 1 };
DEFINE EVENT e ON c5 THEN { CREATE c6 SET x = 1 };
DEFINE EVENT e ON c6 THEN { CREATE c7 SET x = 1 };
DEFINE EVENT e ON c7 THEN { CREATE c8 SET x = 1 };
DEFINE EVENT e ON c8 THEN { CREATE c9 SET x = 1 };
DEFINE EVENT e ON c9 THEN { CREATE c10 SET x = 1 };
DEFINE EVENT e ON c10 THEN { CREATE c11 SET x = 1 };
DEFINE EVENT e ON c11 THEN { CREATE c12 SET x = 1 };
DEFINE EVENT e ON c12 THEN { CREATE c13 SET x = 1 };
DEFINE EVENT e ON c13 THEN { CREATE c14 SET x = 1 };
DEFINE EVENT e ON c14 THEN { CREATE c15 SET x = 1 };
DEFINE EVENT e ON c15 THEN { CREATE c16 SET x = 1 };
DEFINE EVENT e ON c16 THEN { CREATE c17 SET x = 1 };
DEFINE EVENT e ON c17 THEN { CREATE c18 SET x = 1 };
DEFINE EVENT e ON c18 THEN { CREATE c19 SET x = 1 };
DEFINE EVENT e ON c19 THEN { CREATE c20 SET x = 1 };
DEFINE EVENT e ON c20 THEN { CREATE c21 SET x = 1 };
DEFINE EVENT e ON c21 THEN { CREATE c22 SET x = 1 };
DEFINE EVENT e ON c22 THEN { CREATE c23 SET x = 1 };
DEFINE EVENT e ON c23 THEN { CREATE c24 SET x = 1 };
DEFINE EVENT e ON c24 THEN { CREATE c25 SET x = 1 };
DEFINE EVENT e ON c25 THEN { CREATE c26 SET x = 1 };
DEFINE EVENT e ON c26 THEN { CREATE c27 SET x = 1 };
DEFINE EVENT e ON c27 THEN { CREATE c28 SET x = 1 };
DEFINE EVENT e ON c28 THEN { CREATE c29 SET x = 1 };
DEFINE EVENT e ON c29 THEN { CREATE c30 SET x = 1 };
CREATE c0:1 SET x = 0;
SELECT * FROM c0;
-- The same chain, whose events never fire.
DEFINE EVENT e ON q0 WHEN false THEN { CREATE q1 SET x = 1 };
DEFINE EVENT e ON q1 WHEN false THEN { CREATE q2 SET x = 1 };
DEFINE EVENT e ON q2 WHEN false THEN { CREATE q3 SET x = 1 };
DEFINE EVENT e ON q3 WHEN false THEN { CREATE q4 SET x = 1 };
DEFINE EVENT e ON q4 WHEN false THEN { CREATE q5 SET x = 1 };
DEFINE EVENT e ON q5 WHEN false THEN { CREATE q6 SET x = 1 };
DEFINE EVENT e ON q6 WHEN false THEN { CREATE q7 SET x = 1 };
DEFINE EVENT e ON q7 WHEN false THEN { CREATE q8 SET x = 1 };
DEFINE EVENT e ON q8 WHEN false THEN { CREATE q9 SET x = 1 };
DEFINE EVENT e ON q9 WHEN false THEN { CREATE q10 SET x = 1 };
DEFINE EVENT e ON q10 WHEN false THEN { CREATE q11 SET x = 1 };
DEFINE EVENT e ON q11 WHEN false THEN { CREATE q12 SET x = 1 };
DEFINE EVENT e ON q12 WHEN false THEN { CREATE q13 SET x = 1 };
DEFINE EVENT e ON q13 WHEN false THEN { CREATE q14 SET x = 1 };
DEFINE EVENT e ON q14 WHEN false THEN { CREATE q15 SET x = 1 };
DEFINE EVENT e ON q15 WHEN false THEN { CREATE q16 SET x = 1 };
DEFINE EVENT e ON q16 WHEN false THEN { CREATE q17 SET x = 1 };
DEFINE EVENT e ON q17 WHEN false THEN { CREATE q18 SET x = 1 };
DEFINE EVENT e ON q18 WHEN false THEN { CREATE q19 SET x = 1 };
DEFINE EVENT e ON q19 WHEN false THEN { CREATE q20 SET x = 1 };
DEFINE EVENT e ON q20 WHEN false THEN { CREATE q21 SET x = 1 };
DEFINE EVENT e ON q21 WHEN false THEN { CREATE q22 SET x = 1 };
DEFINE EVENT e ON q22 WHEN false THEN { CREATE q23 SET x = 1 };
DEFINE EVENT e ON q23 WHEN false THEN { CREATE q24 SET x = 1 };
DEFINE EVENT e ON q24 WHEN false THEN { CREATE q25 SET x = 1 };
DEFINE EVENT e ON q25 WHEN false THEN { CREATE q26 SET x = 1 };
DEFINE EVENT e ON q26 WHEN false THEN { CREATE q27 SET x = 1 };
DEFINE EVENT e ON q27 WHEN false THEN { CREATE q28 SET x = 1 };
DEFINE EVENT e ON q28 WHEN false THEN { CREATE q29 SET x = 1 };
DEFINE EVENT e ON q29 WHEN false THEN { CREATE q30 SET x = 1 };
CREATE q0:1 SET x = 0;
