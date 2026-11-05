/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view's rows are computed from its source, so a `CREATE` against
it is refused whatever record id it names, and the refusal leaves the rows the
source computed as they were. The check is the creating order's own, ahead of
the data the statement carries.
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
-- A record id that names a group the view does not hold.
CREATE by_city:['Nowhere'] SET city = 'Nowhere', n = 1;
-- A generated record id.
CREATE by_city SET city = 'Nowhere', n = 1;
-- The view still holds only the rows its query computed.
SELECT * FROM by_city;
