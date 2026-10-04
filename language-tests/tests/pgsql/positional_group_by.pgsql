/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL positional GROUP BY 1 and ORDER BY 2"

[[test.results]]
value = "[{ category: 'electronics', count: 2 }, { category: 'books', count: 1 }]"
*/

SELECT p.category, count(p.price) AS count FROM products p GROUP BY 1 ORDER BY 2 DESC;
