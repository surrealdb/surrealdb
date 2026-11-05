/**
[env]
namespace = true
database = true

[test]
reason = """
	ALTER FIELD names the components of an id that had none, changes or drops
	them, and holds the result to the rules DEFINE FIELD keeps: a component
	type a key range can use, no SELECT permission of its own, no field of the
	same name and no reader of a name it drops. A refused ALTER changes nothing.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The record id component `temp` of `reading` cannot have the type `float`: only string, int, datetime, uuid, duration, bool and record components keep their order in storage"

[[test.results]]
error = "Cannot restrict SELECT on the `id` field of `reading`: its record id components are readable as fields"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: \"DEFINE FIELD id ON reading TYPE [city: string] COMMENT 'one reading per city and time' PERMISSIONS FULL\" }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: \"DEFINE FIELD id ON reading TYPE string COMMENT 'one reading per city and time' PERMISSIONS FULL\" }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `place` in the record id of `trip`: `place` is already a field of `trip`"
*/

-- An id with no components, then one that names them.
DEFINE FIELD id ON reading TYPE string;
ALTER FIELD id ON reading TYPE [city: string, time: datetime];
INFO FOR TABLE reading;

-- The same structure with a clause added.
ALTER FIELD id ON reading COMMENT 'one reading per city and time';

-- Refused, and nothing changes.
ALTER FIELD id ON reading TYPE [city: string, temp: float];
ALTER FIELD id ON reading PERMISSIONS NONE;

-- Dropping a component, then all of them.
ALTER FIELD id ON reading TYPE [city: string];
INFO FOR TABLE reading;
ALTER FIELD id ON reading TYPE string;
INFO FOR TABLE reading;

-- A field already holds the name.
DEFINE FIELD place ON trip TYPE string;
DEFINE FIELD id ON trip TYPE string;
ALTER FIELD id ON trip TYPE [place: string, time: datetime];
