/**
[test]
reason = "Test CountScan optimization: SELECT count() FROM table GROUP ALL uses txn.count() instead of full scan"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "{ count: 5 }"

*/

-- Setup: create test data
{
	DEFINE TABLE count_test SCHEMAFULL;
	DEFINE FIELD name ON TABLE count_test TYPE string;
	CREATE count_test:1 SET name = "Alice";
	CREATE count_test:2 SET name = "Bob";
	CREATE count_test:3 SET name = "Charlie";
	CREATE count_test:4 SET name = "Diana";
	CREATE count_test:5 SET name = "Eve";

	DEFINE TABLE empty_count_test SCHEMAFULL;
	RETURN "OK";
};

-- Test 1: Basic COUNT with GROUP ALL (should use CountScan)
SELECT count() FROM count_test GROUP ALL;

-- Test 2: COUNT on empty table with GROUP ALL
-- GROUP ALL on empty table without active perms should produce { count: 0 }
SELECT count() FROM empty_count_test GROUP ALL;

-- Test 3: COUNT with explicit count() alias (still count-all-only)
SELECT count() AS count FROM count_test GROUP ALL;

-- Test 4: COUNT with range source and GROUP ALL
SELECT count() FROM count_test:2..5 GROUP ALL;

-- Test 5: COUNT with ONLY modifier
SELECT count() FROM ONLY count_test GROUP ALL;
