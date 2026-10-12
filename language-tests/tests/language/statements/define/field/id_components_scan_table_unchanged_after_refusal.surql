/**
[env]
namespace = true
database = true

[test]
reason = """
	A refused DEFINE FIELD leaves the table exactly as it was: the catalog has
	no `id` definition, every record is still there under its own id, and the
	statement can be made again once the record that stopped it is gone.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the existing record reading:5 does not match it (the record id is not an array). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[reading:5, reading:['London', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"
*/

CREATE reading:5 SET temp = 1 RETURN NONE;
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 2 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

INFO FOR TABLE reading;
SELECT VALUE temp FROM reading ORDER BY temp;
SELECT VALUE id FROM reading ORDER BY id;

DELETE reading:5 RETURN NONE;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
