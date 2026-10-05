/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL aggregate COUNT and SUM queries"

[[test.results]]
value = "[{ cnt: 3, total_sum: 450 }]"

[[test.results]]
value = "[{ distinct_categories: 2 }]"

[[test.results]]
value = "[{ category: 'electronics', cat_sum: 800 }]"

[[test.results]]
value = "[{ high_price_count: 2 }]"
*/

SELECT count(*) AS cnt, sum(o.total) AS total_sum FROM orders o;

SELECT count(DISTINCT p.category) AS distinct_categories FROM products p;

SELECT p.category, sum(p.price) AS cat_sum FROM products p GROUP BY p.category HAVING sum(p.price) > 50 ORDER BY cat_sum DESC;

SELECT count(*) FILTER (WHERE p.price > 100) AS high_price_count FROM products p;
