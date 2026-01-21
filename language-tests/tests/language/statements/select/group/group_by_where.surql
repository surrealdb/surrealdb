/**
[test]
reason = "Test GROUP BY with WHERE clause for filtering before grouping"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ category: 'books', count: 2, total: 50f }, { category: 'clothing', count: 1, total: 50f }, { category: 'electronics', count: 2, total: 300f }]"

[[test.results]]
value = "[{ avg_price: 30f, category: 'books' }, { avg_price: 50f, category: 'clothing' }, { avg_price: 150f, category: 'electronics' }]"

[[test.results]]
value = "[]"

*/

-- Setup data
{
	CREATE product:1 SET category = 'electronics', price = 100, in_stock = true;
	CREATE product:2 SET category = 'electronics', price = 200, in_stock = true;
	CREATE product:3 SET category = 'electronics', price = 150, in_stock = false;
	CREATE product:4 SET category = 'books', price = 20, in_stock = true;
	CREATE product:5 SET category = 'books', price = 30, in_stock = true;
	CREATE product:6 SET category = 'books', price = 25, in_stock = false;
	CREATE product:7 SET category = 'clothing', price = 50, in_stock = true;
	CREATE product:8 SET category = 'clothing', price = 75, in_stock = false;
	RETURN "OK";
};

-- Test WHERE filtering before GROUP BY
SELECT category, COUNT() as count, math::sum(price) as total FROM product WHERE in_stock = true GROUP BY category;

-- Test WHERE with multiple conditions
SELECT category, math::mean(price) as avg_price FROM product WHERE in_stock = true AND price > 25 GROUP BY category;

-- Test WHERE with no matching records for a group
SELECT category, COUNT() as count FROM product WHERE price > 1000 GROUP BY category;
