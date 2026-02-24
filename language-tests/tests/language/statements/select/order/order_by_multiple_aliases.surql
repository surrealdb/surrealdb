/**
[test]
reason = "Test ORDER BY with multiple alias references"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ a: 1, b: 30, id: t:1 }, { a: 1, b: 20, id: t:2 }, { a: 2, b: 10, id: t:3 }]"

[[test.results]]
value = "[{ a: 1, b: 20, id: t:2 }, { a: 1, b: 30, id: t:1 }, { a: 2, b: 10, id: t:3 }]"

*/

-- Setup: Create test data with multiple sortable fields
{
	CREATE t:1 SET x = 1, y = 30;
	CREATE t:2 SET x = 1, y = 20;
	CREATE t:3 SET x = 2, y = 10;
	RETURN "OK";
};

-- Test 1: ORDER BY single computed alias
SELECT id, x AS a, y AS b FROM t ORDER BY a ASC, b DESC;

-- Test 2: ORDER BY multiple computed aliases
SELECT id, x AS a, y AS b FROM t ORDER BY a ASC, b ASC;
