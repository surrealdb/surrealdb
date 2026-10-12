/**
[env]
namespace = true
database = true

[test]
reason = """
	INSERT takes the key of a row on a table whose `id` names its components
	from the id the row names or, failing that, from the components it carries,
	in every spelling of the statement. The record keeps none of the fields that
	name a component, a row that restates its key passes, and one that
	contradicts it is refused. A row whose key cannot be made is refused before
	IGNORE can skip it, because IGNORE skips a record that already exists and
	not a row that has no key. On a duplicate key the clause that updates the
	record is held to the key as an update is.
"""

# The setup.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# The key from the components of a row, of several rows and of the field list.
[[test.results]]
value = "[reading:['London', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['London', d'2024-01-02T00:00:00Z'], reading:['Paris', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['Rome', d'2024-01-01T00:00:00Z']]"

# An id the row names.
[[test.results]]
value = "[reading:['Oslo', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['Oslo', d'2024-01-02T00:00:00Z']]"

[[test.results]]
error = "Found 'Bergen' for the `city` field, but it is part of the record id of `reading` and does not match it"

# A duplicate key.
[[test.results]]
match = "$error = /Database record .reading:.*. already exists/"
error = true

[[test.results]]
value = "[]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[11]"

[[test.results]]
error = "Found 'Paris' for the `city` field, but it is part of the record id of `reading` and does not match it"

# IGNORE does not skip a row that has no key, nor one whose key is refused.
[[test.results]]
error = "Cannot create a record in `reading` without the record id component `city`: set `city`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
error = "Cannot create a record in `reading` without the record id component `time`: set `time`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
match = "$error = /Couldn't coerce value for the record id component .time. of .reading.: /"
error = true

[[test.results]]
match = "$error = /Couldn't coerce value for the record id component .time. of .reading.: /"
error = true

# The refused rows stored nothing, and the updates changed one record.
[[test.results]]
value = "[{ temp: 2 }, { temp: 3 }, { temp: 4 }, { temp: 5 }, { temp: 6 }, { temp: 11 }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

INSERT INTO reading { city: 'London', time: d'2024-01-01T00:00:00Z', temp: 1 } RETURN VALUE id;
INSERT INTO reading [{ city: 'London', time: d'2024-01-02T00:00:00Z', temp: 2 }, { city: 'Paris', time: d'2024-01-01T00:00:00Z', temp: 3 }] RETURN VALUE id;
INSERT INTO reading (city, time, temp) VALUES ('Rome', d'2024-01-01T00:00:00Z', 4) RETURN VALUE id;

INSERT INTO reading { id: reading:['Oslo', d'2024-01-01T00:00:00Z'], temp: 5 } RETURN VALUE id;
INSERT INTO reading { id: reading:['Oslo', d'2024-01-02T00:00:00Z'], city: 'Oslo', temp: 6 } RETURN VALUE id;
INSERT INTO reading { id: reading:['Oslo', d'2024-01-03T00:00:00Z'], city: 'Bergen', temp: 7 };

INSERT INTO reading { city: 'London', time: d'2024-01-01T00:00:00Z', temp: 10 };
INSERT IGNORE INTO reading { city: 'London', time: d'2024-01-01T00:00:00Z', temp: 10 };
INSERT INTO reading { city: 'London', time: d'2024-01-01T00:00:00Z', temp: 10 } ON DUPLICATE KEY UPDATE temp = $input.temp RETURN VALUE temp;
INSERT INTO reading { city: 'London', time: d'2024-01-01T00:00:00Z', temp: 11 } ON DUPLICATE KEY UPDATE city = 'London', temp = $input.temp RETURN VALUE temp;
INSERT INTO reading { city: 'London', time: d'2024-01-01T00:00:00Z', temp: 12 } ON DUPLICATE KEY UPDATE city = 'Paris';

INSERT IGNORE INTO reading { temp: 1 };
INSERT IGNORE INTO reading { city: 'London', temp: 1 };
INSERT INTO reading { city: 'London', time: '2024-01-01T00:00:00Z' };
INSERT IGNORE INTO reading { city: 'London', time: '2024-01-01T00:00:00Z' };

SELECT temp FROM reading ORDER BY temp;
