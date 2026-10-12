/**
[env]
namespace = true
database = true

[test]
reason = """
	DEFINE FIELD OVERWRITE with the structure the id already has changes only
	what the other clauses say. The records were checked when the components
	were named, and they are not touched.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "{ events: {  }, fields: { id: \"DEFINE FIELD id ON reading TYPE [city: string, time: datetime] COMMENT 'one reading per city and time' PERMISSIONS FULL\" }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 1 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 2 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime] COMMENT 'one reading per city and time';

SELECT VALUE temp FROM reading ORDER BY temp;
INFO FOR TABLE reading;
