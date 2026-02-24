/**
[test]
reason = "Test ORDER BY referencing SELECT aliases"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ doubled: 2, id: t:1, val: 1 }, { doubled: 4, id: t:2, val: 2 }, { doubled: 6, id: t:3, val: 3 }]"

[[test.results]]
value = "[{ doubled: 6, id: t:3, val: 3 }, { doubled: 4, id: t:2, val: 2 }, { doubled: 2, id: t:1, val: 1 }]"

[[test.results]]
value = "[{ computed: 'A-1', id: t:1, name: 'A' }, { computed: 'B-2', id: t:2, name: 'B' }, { computed: 'C-3', id: t:3, name: 'C' }]"

*/

-- Setup: Create test data
{
	CREATE t:1 SET val = 1, name = "A";
	CREATE t:2 SET val = 2, name = "B";
	CREATE t:3 SET val = 3, name = "C";
	RETURN "OK";
};

-- Test 1: ORDER BY computed expression alias (ASC)
SELECT id, val, val * 2 AS doubled FROM t ORDER BY doubled ASC;

-- Test 2: ORDER BY computed expression alias (DESC)
SELECT id, val, val * 2 AS doubled FROM t ORDER BY doubled DESC;

-- Test 3: ORDER BY string concatenation alias
SELECT id, name, string::concat(name, "-", <string>val) AS computed FROM t ORDER BY computed ASC;
