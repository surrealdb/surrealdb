/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "SELECT alias that matches a source field name should not shadow the original field for other projections"
issue = 7161

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: bug_relate:1, other_id: test:a }]"

[[test.results]]
value = "[{ id: bug_relate:1, other_id: test:a }]"

[[test.results]]
value = "[{ linked_id: bug_relate:1, other_id: test:a }]"
*/

-- Setup
{
	DEFINE TABLE bug_relate TYPE NORMAL;
	DEFINE TABLE test TYPE NORMAL;
	DEFINE FIELD bug_relate ON test TYPE record<bug_relate>;
	CREATE bug_relate:1;
	CREATE test:a SET bug_relate = bug_relate:1;
	RETURN "OK";
};

-- Test 1: Computed alias first, simple field second (original bug report)
SELECT bug_relate.id AS id, id AS other_id FROM test;

-- Test 2: Simple field first, computed alias second (reversed order)
SELECT id AS other_id, bug_relate.id AS id FROM test;

-- Test 3: No shadowing (different alias name) — control case
SELECT bug_relate.id AS linked_id, id AS other_id FROM test;
