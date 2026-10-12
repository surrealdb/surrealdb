/**
[env]
namespace = true
database = true

[test]
reason = """
	DEFINE FIELD IF NOT EXISTS over an id that is already defined changes
	nothing, so it neither names the components nor checks the records, even
	though a record the new structure would refuse is there.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE any PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:5 SET temp = 1 RETURN NONE;
DEFINE FIELD id ON reading TYPE any;
DEFINE FIELD IF NOT EXISTS id ON reading TYPE [city: string, time: datetime];

INFO FOR TABLE reading;
