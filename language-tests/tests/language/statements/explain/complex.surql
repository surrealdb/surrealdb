/**
[test]
reason = "Test EXPLAIN with complex queries that combine multiple planner operators into deep plans"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Limit [ctx: Db] [limit: 2]
    SortTopK [ctx: Db] [order_by: revenue DESC, limit: 2]
        Aggregate [ctx: Db] [by: category]
            TableScan [ctx: Db] [table: product, direction: Forward]
''''

[[test.results]]
value = ''''Project [ctx: Db]
    Limit [ctx: Db] [limit: 5]
        SortTopKByKey [ctx: Db] [sort_keys: rating DESC, limit: 5]
            Split [ctx: Db] [on: tags]
                TableScan [ctx: Db] [table: product, direction: Forward, predicate: price < 50]
''''

[[test.results]]
value = '''"Project [ctx: Db]
    Limit [ctx: Db] [limit: 1, offset: 0]
        SortTopKByKey [ctx: Db] [sort_keys: price ASC, limit: 1]
            Filter [ctx: Db] [predicate: category = 'electronics' AND brand = 'Acme']
                IndexScan [ctx: Db] [index: idx_category, access: = 'electronics', direction: Forward]
"'''

[[test.results]]
value = '''"Project [ctx: Db]
    Filter [ctx: Db] [predicate: name @1@ 'Widget']
        FullTextScan [ctx: Db] [index: idx_ft_name, query: Widget]
"'''

[[test.results]]
value = '''"Aggregate [ctx: Db] [mode: GROUP ALL]
    Filter [ctx: Db] [predicate: category = 'electronics']
        IndexScan [ctx: Db] [index: idx_category, access: = 'electronics', direction: Forward]
"'''

[[test.results]]
value = ''''Project [ctx: Db]
    Limit [ctx: Db] [limit: 5]
        SortTopKByKey [ctx: Db] [sort_keys: id ASC, limit: 5]
            Union [ctx: Db]
                TableScan [ctx: Db] [table: product, direction: Forward]
                TableScan [ctx: Db] [table: review, direction: Forward]
''''

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: name]
    TableScan [ctx: Db] [table: product, direction: Forward, predicate: rating >= 4f, limit: 3]
''''

[[test.results]]
value = '''"Project [ctx: Db]
    Filter [ctx: Db] [predicate: tags = ['sale']]
        Project [ctx: Db]
            Filter [ctx: Db] [predicate: category = 'electronics']
                Union [ctx: Db]
                    Project [ctx: Db]
                        Filter [ctx: Db] [predicate: rating >= 4f]
                            Union [ctx: Db]
                                TableScan [ctx: Db] [table: product, direction: Forward]
                                TableScan [ctx: Db] [table: review, direction: Forward]
                    IndexScan [ctx: Db] [index: idx_category, access: = 'electronics', direction: Forward]
"'''
*/

-- Setup schema with indexes and realistic data
{
	DEFINE INDEX idx_category ON product FIELDS category;
	DEFINE INDEX idx_cat_brand ON product FIELDS category, brand;
	DEFINE ANALYZER product_search TOKENIZERS blank, class;
	DEFINE INDEX idx_ft_name ON product FIELDS name FULLTEXT ANALYZER product_search BM25;

	CREATE product:1  SET name = 'Ultra Widget',    category = 'electronics', brand = 'Acme',  price = 29.99, tags = ['sale', 'new'],       rating = 4.5;
	CREATE product:2  SET name = 'Mega Widget',     category = 'electronics', brand = 'Acme',  price = 49.99, tags = ['popular'],            rating = 4.2;
	CREATE product:3  SET name = 'Basic Widget',    category = 'electronics', brand = 'Beta',  price = 9.99,  tags = ['sale', 'clearance'],  rating = 3.8;
	CREATE product:4  SET name = 'Pro Gadget',      category = 'electronics', brand = 'Beta',  price = 99.99, tags = ['new', 'premium'],     rating = 4.9;
	CREATE product:5  SET name = 'Comfy Chair',     category = 'furniture',   brand = 'Gamma', price = 199.99, tags = ['sale'],              rating = 4.0;
	CREATE product:6  SET name = 'Solid Desk',      category = 'furniture',   brand = 'Gamma', price = 299.99, tags = ['premium'],           rating = 4.7;
	CREATE product:7  SET name = 'Soft Pillow',     category = 'furniture',   brand = 'Delta', price = 19.99, tags = ['sale', 'popular'],    rating = 3.5;
	CREATE product:8  SET name = 'Widget Accessory', category = 'accessories', brand = 'Acme', price = 5.99,  tags = ['new'],                rating = 4.1;
	CREATE product:9  SET name = 'Gadget Case',     category = 'accessories', brand = 'Beta',  price = 14.99, tags = ['popular', 'sale'],    rating = 3.9;
	CREATE product:10 SET name = 'Premium Bundle',  category = 'accessories', brand = 'Acme',  price = 39.99, tags = ['premium', 'new'],     rating = 4.6;

	CREATE review:1 SET product = product:1, score = 5, text = 'Great widget';
	CREATE review:2 SET product = product:2, score = 3, text = 'Decent widget';
	CREATE review:3 SET product = product:4, score = 5, text = 'Best gadget ever';

	RETURN "OK";
};

-- Complex query 1: GROUP BY + ORDER BY + LIMIT on grouped aggregation
-- Exercises: Limit > Sort > Aggregate > Scan
EXPLAIN SELECT
	category,
	count() AS cnt,
	math::sum(price) AS revenue,
	math::mean(rating) AS avg_rating
FROM product
GROUP BY category
ORDER BY revenue DESC
LIMIT 2;

-- Complex query 2: predicate + SPLIT + ORDER BY + LIMIT
-- Exercises: Limit > Sort > Split > Scan(predicate)
EXPLAIN SELECT
	*
FROM product
WHERE price < 50
SPLIT tags
ORDER BY rating DESC
LIMIT 5;

-- Complex query 3: compound predicate + ORDER BY + LIMIT + START
-- Exercises: Limit(offset) > Sort > Scan(compound predicate)
EXPLAIN SELECT
	name, price, rating
FROM product
WHERE category = 'electronics' AND brand = 'Acme'
ORDER BY price ASC
LIMIT 1
START 0;

-- Complex query 4: full-text search predicate
-- Exercises: Scan with fulltext predicate
EXPLAIN SELECT
	id, name
FROM product
WHERE name @1@ 'Widget';

-- Complex query 5: GROUP ALL with multiple aggregation functions and predicate
-- Exercises: Aggregate(GROUP ALL) > Scan(predicate)
EXPLAIN SELECT
	count() AS cnt,
	math::sum(price) AS total,
	math::min(price) AS cheapest,
	math::max(price) AS priciest
FROM product
WHERE category = 'electronics'
GROUP ALL;

-- Complex query 6: multiple FROM tables + ORDER BY + LIMIT
-- Exercises: Limit > Sort > Union(Scan, Scan)
EXPLAIN SELECT
	*
FROM product, review
ORDER BY id ASC
LIMIT 5;

-- Complex query 7: SELECT VALUE with predicate and LIMIT
-- Exercises: ProjectValue > Scan(predicate, limit)
EXPLAIN SELECT VALUE name
FROM product
WHERE rating >= 4.0
LIMIT 3;

-- Complex query 8: Multiple subqueries.
EXPLAIN SELECT * FROM (SELECT * FROM (SELECT * FROM product, review WHERE rating >= 4.0), product WHERE category = 'electronics') WHERE tags = ['sale'];
