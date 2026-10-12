/**
[env]
namespace = true
database = true

[test]
reason = """
	The record id of a view is derived from the view's query, so its parts
	cannot be named, whether the view aggregates or copies its source.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name the parts of the record id of `by_city`: it is a view"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name the parts of the record id of `by_sale`: it is a view"
*/

DEFINE TABLE sale;
DEFINE TABLE by_city AS SELECT city, count() AS n FROM sale GROUP BY city;
DEFINE FIELD id ON by_city TYPE [city: string, time: datetime];

DEFINE TABLE by_sale AS SELECT * FROM sale;
DEFINE FIELD id ON by_sale TYPE [city: string, time: datetime];
