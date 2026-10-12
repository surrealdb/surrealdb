/**
[env]
namespace = true
database = true

[test]
reason = """
	Every record id the table holds must fit the named structure. Datetimes
	with a sub-second part and without one both fit a datetime component, and
	the records are left as they were.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[4, 5, 6, 7]"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 4 RETURN NONE;
CREATE reading:['London', d'2024-01-01T00:00:00.123456Z'] SET temp = 5 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:01Z'] SET temp = 6 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:01.5Z'] SET temp = 7 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

SELECT VALUE temp FROM reading ORDER BY temp;
INFO FOR TABLE reading;
