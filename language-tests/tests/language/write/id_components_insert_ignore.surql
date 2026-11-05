/**
[env]
namespace = true
database = true

[test]
reason = """
	INSERT IGNORE skips only a conflict with a record that exists, on a table
	whose id names its components as on any other. A row whose fields contradict
	its record id was rejected, not found to exist: IGNORE raises that error as it
	raises a field the type refuses or an assertion that fails, and the error
	fails the whole statement, the rows around it included. A row that has no key
	to attempt is refused before any attempt is made, whatever IGNORE says. A key
	that already exists is a conflict, and IGNORE skips it.
"""

# The setup.
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

# A field the type refuses: an error with IGNORE as without it.
[[test.results]]
match = "$error = /Couldn't coerce value for field .n. of .plain:1.: /"
error = true

[[test.results]]
match = "$error = /Couldn't coerce value for field .n. of .plain:1.: /"
error = true

# A field that contradicts the record id: the same.
[[test.results]]
error = "Found 'Bergen' for the `city` field, but it is part of the record id of `reading` and does not match it"

[[test.results]]
error = "Found 'Bergen' for the `city` field, but it is part of the record id of `reading` and does not match it"

# An assertion that fails: the same.
[[test.results]]
match = "$error = /Found 500 for field .temp./"
error = true

# A key that cannot be made is refused, IGNORE or not.
[[test.results]]
error = "Cannot create a record in `reading` without the record id component `city`: set `city`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
error = "Cannot create a record in `reading` without the record id component `time`: set `time`, give the record id, or define a DEFAULT on the `id` field"

# A contradicting row fails the whole statement: the row after it is not inserted.
[[test.results]]
error = "Found 'Bergen' for the `city` field, but it is part of the record id of `reading` and does not match it"

[[test.results]]
value = "[]"

# A row that conflicts with nothing is inserted, and a key that exists is skipped.
[[test.results]]
value = "[reading:['Rome', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[]"

# What was stored.
[[test.results]]
value = "[{ city: 'Rome', id: reading:['Rome', d'2024-01-01T00:00:00Z'], temp: 2, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading ASSERT $value < 100;
DEFINE TABLE plain SCHEMALESS;
DEFINE FIELD n ON plain TYPE int;

-- A field the type refuses.
INSERT IGNORE INTO plain { id: plain:1, n: 'x' };
INSERT INTO plain { id: plain:1, n: 'x' };

-- A field that contradicts the id the row names.
INSERT IGNORE INTO reading { id: reading:['Oslo', d'2024-01-03T00:00:00Z'], city: 'Bergen', temp: 7 };
INSERT INTO reading { id: reading:['Oslo', d'2024-01-03T00:00:00Z'], city: 'Bergen', temp: 7 };

-- An assertion that fails.
INSERT IGNORE INTO reading { city: 'Oslo', time: d'2024-01-05T00:00:00Z', temp: 500 };

-- A row with no key to attempt.
INSERT IGNORE INTO reading { temp: 1 };
INSERT IGNORE INTO reading { city: 'London', temp: 1 };

-- The contradicting row fails the statement and nothing is inserted.
INSERT IGNORE INTO reading [
	{ id: reading:['Oslo', d'2024-01-03T00:00:00Z'], city: 'Bergen', temp: 7 },
	{ city: 'Rome', time: d'2024-01-01T00:00:00Z', temp: 2 }
] RETURN VALUE id;
SELECT * FROM reading;

-- A row that conflicts with nothing is inserted; a key that exists is a conflict, which IGNORE skips.
INSERT IGNORE INTO reading { city: 'Rome', time: d'2024-01-01T00:00:00Z', temp: 2 } RETURN VALUE id;
INSERT IGNORE INTO reading { city: 'Rome', time: d'2024-01-01T00:00:00Z', temp: 3 };

SELECT * FROM reading;
SELECT n FROM plain;
