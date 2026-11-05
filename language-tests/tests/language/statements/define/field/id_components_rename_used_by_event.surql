/**
[env]
namespace = true
database = true

[test]
reason = """
	An event whose condition reads `city` reads the component of the record id,
	so the record id cannot drop or rename that name until the event is removed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `reading`: the event `london` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `reading`: the event `london` reads its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE EVENT london ON reading WHEN city = 'London' THEN {};

ALTER FIELD id ON reading TYPE [place: string, time: datetime];
REMOVE FIELD id ON reading;

REMOVE EVENT london ON reading;
REMOVE FIELD id ON reading;
