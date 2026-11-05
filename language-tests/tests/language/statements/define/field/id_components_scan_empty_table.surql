/**
[env]
namespace = true
database = true

[test]
reason = """
	Naming the components of a record id checks the records the table already
	holds. A table with none, whether it was defined or only implied, has
	nothing to refuse.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE TABLE reading;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- A table no statement has defined yet.
DEFINE FIELD id ON trip TYPE [city: string, time: datetime];

INFO FOR TABLE reading;
