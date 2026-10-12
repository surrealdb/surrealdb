/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view's rows are computed from its source, so an `UPSERT` against
it is refused in both of its orders: the one that creates the record and the one
that updates an existing record. A condition on a group column reads the view
through its key range, and whichever order the rows it selects lead to is
refused.
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
UPSERT by_city:['Nowhere'] SET city = 'Nowhere', n = 1;
-- The updating order: the record exists.
UPSERT by_city:['London'] SET n = 0;
-- A condition that selects a row of the view reaches the updating order.
UPSERT by_city SET n = 0 WHERE city = 'London';
-- A condition that selects none creates a record, which is refused.
UPSERT by_city SET n = 0 WHERE city = 'Nowhere';
-- The view still holds only the rows its query computed.
SELECT * FROM by_city;
