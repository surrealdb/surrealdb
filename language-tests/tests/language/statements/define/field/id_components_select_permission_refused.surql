/**
[env]
namespace = true
database = true

[test]
reason = """
	The components of a record id are readable as fields of the table, which
	the table's permissions govern, so the `id` field that names them takes no
	permission of its own on SELECT. An `id` that names nothing is unaffected.
"""

[[test.results]]
error = "Cannot restrict SELECT on the `id` field of `reading`: its record id components are readable as fields"

[[test.results]]
error = "Cannot restrict SELECT on the `id` field of `reading`: its record id components are readable as fields"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot restrict SELECT on the `id` field of `reading`: its record id components are readable as fields"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FOR select WHERE city = 'London';
DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS NONE;

-- A full SELECT permission is the one that names components.
DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FOR select FULL;

-- ALTER FIELD keeps the rule for the id it names.
ALTER FIELD id ON reading PERMISSIONS NONE;

-- An id that names no components may restrict SELECT.
DEFINE FIELD id ON plain TYPE string PERMISSIONS NONE;
