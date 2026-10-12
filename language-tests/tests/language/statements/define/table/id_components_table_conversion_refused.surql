/**
[env]
namespace = true
database = true

[test]
reason = """
	A table whose record id names its components cannot become a relation
	table or a view: their record ids have no components to name. DEFINE TABLE
	OVERWRITE and ALTER TABLE both refuse, while a change that keeps the table
	a plain table is allowed, and a table that names no components converts
	freely.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot make `reading` a relation table: its record id has named components"

[[test.results]]
error = "Cannot make `reading` a view: its record id has named components"

[[test.results]]
error = "Cannot make `reading` a relation table: its record id has named components"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE source;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

DEFINE TABLE OVERWRITE reading TYPE RELATION;
DEFINE TABLE OVERWRITE reading AS SELECT * FROM source;
ALTER TABLE reading TYPE RELATION;

-- Changes that keep it a plain table.
DEFINE TABLE OVERWRITE reading SCHEMAFULL;
ALTER TABLE reading SCHEMALESS;

-- A table with an unnamed id converts.
DEFINE FIELD id ON plain TYPE string;
DEFINE TABLE OVERWRITE plain TYPE RELATION;
