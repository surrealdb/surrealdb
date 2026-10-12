/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view's rows are computed from its source, so an `INSERT` against
it is refused in both of its orders: the one that creates the record and the one
that rewrites a record whose id already exists. Neither leaves the view changed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'London', id: by_city:['London'], n: 2 }, { city: 'Paris', id: by_city:['Paris'], n: 1 }]"

[[test.results]]
error = "Cannot write to the `by_city` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "Cannot write to the `by_city` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
value = "[{ city: 'London', id: by_city:['London'], n: 2 }, { city: 'Paris', id: by_city:['Paris'], n: 1 }]"

*/
DEFINE TABLE sale SCHEMALESS;
DEFINE TABLE by_city AS SELECT city, count() AS n FROM sale GROUP BY city;
INSERT INTO sale [
	{ id: 1, city: 'London' },
	{ id: 2, city: 'London' },
	{ id: 3, city: 'Paris' }
] RETURN NONE;
-- The view holds a row per city.
SELECT * FROM by_city;
-- The creating order: a record id the view does not hold.
INSERT INTO by_city { id: by_city:['Nowhere'], city: 'Nowhere', n: 1 };
-- The updating order: the record exists, and its duplicate key clause would
-- rewrite it.
INSERT INTO by_city { id: by_city:['London'], city: 'London', n: 0 }
	ON DUPLICATE KEY UPDATE n = 0;
-- The view still holds only the rows its query computed.
SELECT * FROM by_city;
