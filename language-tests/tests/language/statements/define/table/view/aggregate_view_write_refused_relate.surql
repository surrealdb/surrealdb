/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view's rows are computed from its source, so a `RELATE` through
it is refused in both of its orders: the one that creates the edge and the one
that rewrites an edge whose id already exists. The view is a table of any type,
so no check of the table's type comes first.
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
-- A generated record id.
RELATE person:ada->by_city->person:bob;
-- The creating order: a record id the view does not hold.
RELATE person:ada->by_city:['Nowhere']->person:bob;
-- The updating order: the record exists.
RELATE person:ada->by_city:['London']->person:bob;
-- The view still holds only the rows its query computed.
SELECT * FROM by_city;
