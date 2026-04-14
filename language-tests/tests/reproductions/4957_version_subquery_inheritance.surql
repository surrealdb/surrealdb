/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause should propagate from outer SELECT into subqueries"
issue = 4957

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[['changed-name']]"

[[test.results]]
value = "[['origin-name']]"

[[test.results]]
value = "[['origin-name']]"

*/

-- Reproduction test for issue #4957: VERSION clause in outer SELECT should
-- propagate to subqueries that don't have their own VERSION clause.
-- https://github.com/surrealdb/surrealdb/issues/4957
--
-- The two query forms should return the same result:
--   1. VERSION on outer SELECT only
--   2. VERSION on both outer and inner SELECT

-- Schema and first version of data
{
	DEFINE TABLE test_tb SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE test_owner TYPE RELATION IN test_tb OUT test_tb CHANGEFEED 1h;
	CREATE test_tb:0 SET name = 'origin-name';
	CREATE test_tb:1 SET name = 'owner-name';
	RELATE test_tb:0->test_owner:0->test_tb:1;
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- Update the name after the timestamp
{
	UPDATE test_tb:0 SET name = 'changed-name';
	RETURN "OK";
};

-- Query 1 (HEAD): subquery returns changed-name
SELECT value (SELECT value name FROM <-test_owner.in LIMIT 1) FROM test_tb:1;

-- Query 2: VERSION on outer only — subquery should inherit the version
SELECT value (SELECT value name FROM <-test_owner.in LIMIT 1) FROM test_tb:1 VERSION $t1;

-- Query 3: VERSION on both — explicit version on inner, should match Query 2
SELECT value (SELECT value name FROM <-test_owner.in LIMIT 1 VERSION $t1) FROM test_tb:1 VERSION $t1;
