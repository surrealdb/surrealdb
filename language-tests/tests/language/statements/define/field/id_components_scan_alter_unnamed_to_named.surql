/**
[env]
namespace = true
database = true

[test]
reason = """
	ALTER FIELD checks the stored records when it names the components of an
	id that had none, as DEFINE FIELD does: a record the structure refuses
	stops it, and once that record is gone it succeeds.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the existing record reading:5 does not match it (the record id is not an array). Copy the records into a table with the new record id instead"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE FIELD id ON reading TYPE any;
CREATE reading:5 SET temp = 1 RETURN NONE;
ALTER FIELD id ON reading TYPE [city: string, time: datetime];

DELETE reading:5 RETURN NONE;
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 2 RETURN NONE;
ALTER FIELD id ON reading TYPE [city: string, time: datetime];

INFO FOR TABLE reading;
