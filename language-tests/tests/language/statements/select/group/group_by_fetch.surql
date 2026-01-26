/**
[test]
reason = "Test GROUP BY with FETCH clause for retrieving related records after grouping"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ author: { id: author:1, name: 'Alice' }, book_count: 2, total_sales: 250f }, { author: { id: author:2, name: 'Bob' }, book_count: 2, total_sales: 320f }]"

[[test.results]]
value = "[{ author: { id: author:2, name: 'Bob' }, avg_sales: 160f }, { author: { id: author:1, name: 'Alice' }, avg_sales: 125f }]"

*/

-- Setup data
{
	CREATE author:1 SET name = 'Alice';
	CREATE author:2 SET name = 'Bob';
	CREATE book:1 SET title = 'Book A', author = author:1, sales = 100;
	CREATE book:2 SET title = 'Book B', author = author:1, sales = 150;
	CREATE book:3 SET title = 'Book C', author = author:2, sales = 200;
	CREATE book:4 SET title = 'Book D', author = author:2, sales = 120;
	RETURN "OK";
};

-- Test FETCH after GROUP BY
SELECT author, COUNT() as book_count, math::sum(sales) as total_sales FROM book GROUP BY author FETCH author;

-- Test FETCH with ORDER BY on grouped results
SELECT author, math::mean(sales) as avg_sales FROM book GROUP BY author ORDER BY avg_sales DESC FETCH author;
