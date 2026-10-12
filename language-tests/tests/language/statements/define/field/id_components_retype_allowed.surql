/**
[env]
namespace = true
database = true

[test]
reason = """
	A component that keeps its name and position may change its type or gain
	company, even while a permission reads it: the stored records are checked
	against the new structure, and an empty table conforms to any. A name
	nothing reads may be added.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE TABLE reading PERMISSIONS FOR select WHERE city = 'London';
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- The same names and positions under another type.
ALTER FIELD id ON reading TYPE [city: string, time: string];

-- A name added after the others.
ALTER FIELD id ON reading TYPE [city: string, time: string, seq: int];

-- A name dropped from the end that nothing reads, restoring the first type.
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime];

INFO FOR TABLE reading;
