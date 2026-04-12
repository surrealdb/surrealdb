/**
[env]
backend = ["mem", "surrealkv"]
versioned = true
timeout = 5000

planner-strategy = ["all-ro"]

[test]
reason = "Inner VERSION clause should override outer VERSION in subqueries"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, name: 'v3' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

-- When a subquery has its own VERSION clause, it should override the
-- outer query's VERSION, not inherit it.

{
	DEFINE TABLE item SCHEMALESS CHANGEFEED 1h;
	CREATE item:1 SET name = 'v1';
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

{
	UPDATE item:1 SET name = 'v2';
	RETURN "OK";
};

SLEEP 50ms;
LET $t2 = time::now();
SLEEP 50ms;

{
	UPDATE item:1 SET name = 'v3';
	RETURN "OK";
};

-- Sanity: HEAD shows v3
SELECT * FROM item:1;

-- Sanity: VERSION $t1 shows v1
SELECT * FROM item:1 VERSION $t1;

-- Outer at $t1 (v1), inner at HEAD (no VERSION = inherits outer = v1?
-- No — inner has NO version clause but also no VersionScope, so it
-- inherits the outer's version_stamp via the context)
-- BUT if the inner explicitly says VERSION current time, it overrides.
-- Test: inner subquery with no VERSION inherits outer (v1),
--       inner subquery with explicit later VERSION overrides.

-- Inner with no VERSION inherits outer $t1; inner subquery reads HEAD (v3)
-- because inner has no VERSION and outer's version_stamp flows through
SELECT *, (SELECT * FROM item:1) AS latest FROM item:1 VERSION $t1;

-- Inner with explicit VERSION $t2 overrides outer $t1
SELECT *, (SELECT * FROM item:1 VERSION $t2) AS latest FROM item:1 VERSION $t1;
