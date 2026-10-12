/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view's rows are computed from its source, so an `UPDATE` against
it is refused, whether it names the view, one of its records, or a condition on
a group column that reads only part of the view. The refusal precedes the
condition, so a record the scan reaches is refused whether or not the condition
holds. No statement leaves the view changed.
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
-- The whole view.
UPDATE by_city SET n = 0;
-- One record.
UPDATE by_city:['London'] SET n = 0;
-- A condition on a group column, which reads the view through its key range.
-- The refusal is judged on the record the range reaches, ahead of the condition.
UPDATE by_city SET n = 0 WHERE city = 'London';
-- A condition on a computed column.
UPDATE by_city SET n = 0 WHERE n > 1;
-- The view still holds only the rows its query computed.
SELECT * FROM by_city;
