/**
[test]
reason = "Test GROUP BY with array fields and array aggregation functions"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 1, tags: [], total: 30f }, { count: 3, tags: ['a', 'b'], total: 55f }, { count: 1, tags: ['c', 'd'], total: 15f }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ category: 'A', names: ['Product1', 'Product2', 'Product4'] }, { category: 'B', names: ['Product3'] }]"

[[test.results]]
value = "[{ category: 'A', count: 3 }, { category: 'B', count: 1 }]"

*/

-- Setup data
{
	CREATE items:1 SET tags = ['a', 'b'], score = 10;
	CREATE items:2 SET tags = ['a', 'b'], score = 20;
	CREATE items:3 SET tags = ['c', 'd'], score = 15;
	CREATE items:4 SET tags = ['a', 'b'], score = 25;
	CREATE items:5 SET tags = [], score = 30;
	RETURN "OK";
};

-- Test grouping by array field (entire array as key)
SELECT tags, COUNT() as count, math::sum(score) as total FROM items GROUP BY tags;

-- Test array aggregation with GROUP BY
{
	CREATE products:1 SET category = 'A', name = 'Product1';
	CREATE products:2 SET category = 'A', name = 'Product2';
	CREATE products:3 SET category = 'B', name = 'Product3';
	CREATE products:4 SET category = 'A', name = 'Product4';
	RETURN "OK";
};

SELECT category, array::group(name) as names FROM products GROUP BY category;

-- Test array distinct with grouping
SELECT category, COUNT() as count FROM products GROUP BY category;
