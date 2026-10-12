/**
[env]
namespace = true
database = true

[test]
reason = """
	A field whose computed expression reads `city` reads the component of the
	record id, so the record id cannot drop or rename that name until the field
	is removed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `reading`: the field `tag` reads its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE int;
DEFINE FIELD tag ON reading TYPE string COMPUTED string::concat(city, '-', temp);

ALTER FIELD id ON reading TYPE [place: string, time: datetime];

REMOVE FIELD tag ON reading;
ALTER FIELD id ON reading TYPE [place: string, time: datetime];
