/**
[env]
namespace = true
database = true

[test]
reason = """
	A table permission that reads `city` reads the component of the record id,
	so the record id cannot drop or rename that name while the permission stands:
	ALTER FIELD, DEFINE FIELD OVERWRITE and REMOVE FIELD all refuse. A name that
	nothing reads can be renamed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `reading`: the table permission `select` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `reading`: the table permission `select` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `reading`: the table permission `select` reads its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, ts: datetime] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE TABLE reading PERMISSIONS FOR select WHERE city = 'London';
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

ALTER FIELD id ON reading TYPE [place: string, time: datetime];
DEFINE FIELD OVERWRITE id ON reading TYPE [place: string, time: datetime];
REMOVE FIELD id ON reading;

-- `time` is read by nothing.
ALTER FIELD id ON reading TYPE [city: string, ts: datetime];
INFO FOR TABLE reading;
