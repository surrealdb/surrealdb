/**
[test]
reason = "Test ORDER BY with two columns in different directions"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ category: 'A', id: item:1, priority: 3 }, { category: 'A', id: item:2, priority: 1 }, { category: 'B', id: item:3, priority: 2 }, { category: 'B', id: item:4, priority: 1 }]"

[[test.results]]
value = "[{ category: 'A', id: item:2, priority: 1 }, { category: 'A', id: item:1, priority: 3 }, { category: 'B', id: item:4, priority: 1 }, { category: 'B', id: item:3, priority: 2 }]"

[[test.results]]
value = "[{ category: 'B', id: item:4, priority: 1 }, { category: 'B', id: item:3, priority: 2 }, { category: 'A', id: item:2, priority: 1 }, { category: 'A', id: item:1, priority: 3 }]"

*/

-- Setup: Create records with category and priority
{
	CREATE item:1 SET category = "A", priority = 3;
	CREATE item:2 SET category = "A", priority = 1;
	CREATE item:3 SET category = "B", priority = 2;
	CREATE item:4 SET category = "B", priority = 1;
	RETURN "OK";
};

-- Test: Order by category ASC, then priority DESC
SELECT * FROM item ORDER BY category ASC, priority DESC;

-- Test: Order by category ASC, then priority ASC
SELECT * FROM item ORDER BY category ASC, priority ASC;

-- Test: Order by category DESC, then priority ASC
SELECT * FROM item ORDER BY category DESC, priority ASC;
