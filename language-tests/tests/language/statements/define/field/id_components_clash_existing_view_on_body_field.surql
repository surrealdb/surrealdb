/**
[env]
namespace = true
database = true

[test]
reason = """
	A view over the table that selects or groups by `city` reads the stored
	field of that name. Naming `city` in the record id would change what the
	view reads, so the statement is refused until the view is removed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `city` in the record id of `reading`: the view `by_city` already reads a stored `city` field. Remove it, then define it again after changing the record id"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE reading;
DEFINE TABLE by_city AS SELECT city, count() AS n FROM reading GROUP BY city;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

REMOVE TABLE by_city;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
