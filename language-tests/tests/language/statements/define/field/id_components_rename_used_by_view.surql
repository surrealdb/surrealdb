/**
[env]
namespace = true
database = true

[test]
reason = """
	A view that selects or groups by `city` reads the component of the record
	id, so the record id cannot drop or rename that name until the view is
	removed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `by_city` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `by_city` reads its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE reading;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE TABLE by_city AS SELECT city, count() AS n FROM reading GROUP BY city;

ALTER FIELD id ON reading TYPE [place: string, time: datetime];
REMOVE FIELD id ON reading;

REMOVE TABLE by_city;
ALTER FIELD id ON reading TYPE [place: string, time: datetime];
