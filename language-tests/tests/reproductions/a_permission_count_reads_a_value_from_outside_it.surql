/**
[env]
imports = ["reproductions/a_counted_value_survives_a_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = """
	A count under a per-row permission runs the statement's own plan beside
	the count. A plan that reads a value from outside the statement, a `LET`
	the condition or the record range reads, cannot stand beside the count,
	so the planner declines that rewrite and the statement keeps its ordinary
	plan. Declining is a refusal, not a broken IR id, and the count is the
	count of the rows the permission lets through.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1 }]"

*/

LET $one = 1;
SELECT count() FROM part WHERE a = $one GROUP ALL;

SELECT count() FROM part WHERE a = $one AND id != part:1 GROUP ALL;
