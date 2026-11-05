/**
[env]
namespace = true
database = true

[test]
reason = """
	A field cannot take the name of a component of the record id, whatever the
	statement form or the path under the name: `city`, `city.x` and `city[*]`
	are all the component `city`. A field with any other name is unaffected.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define the field `city` on `reading`: `city` is part of the record id of `reading`"

[[test.results]]
error = "Cannot define the field `city` on `reading`: `city` is part of the record id of `reading`"

[[test.results]]
error = "Cannot define the field `city` on `reading`: `city` is part of the record id of `reading`"

[[test.results]]
error = "Cannot define the field `time` on `reading`: `time` is part of the record id of `reading`"

[[test.results]]
error = "Cannot define the field `city` on `reading`: `city` is part of the record id of `reading`"

[[test.results]]
error = "Cannot define the field `city` on `reading`: `city` is part of the record id of `reading`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FULL', temp: 'DEFINE FIELD temp ON reading TYPE int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- The name itself, and a path under it.
DEFINE FIELD city ON reading TYPE string;
DEFINE FIELD city.x ON reading TYPE int;
DEFINE FIELD city[*] ON reading TYPE string;

-- The other component.
DEFINE FIELD time ON reading TYPE datetime;

-- The statement forms that skip or replace an existing definition.
DEFINE FIELD IF NOT EXISTS city ON reading TYPE string;
DEFINE FIELD OVERWRITE city ON reading TYPE string;

-- Any other name is a field as usual.
DEFINE FIELD temp ON reading TYPE int;

INFO FOR TABLE reading;
