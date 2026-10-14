/**
[env]
namespace = true
database = true

[test]
reason = """
	A table whose `id` names its components takes the key of a new record from
	the first source that applies: the id the statement or the record names,
	every component written as a field, or the `id` field's DEFAULT. Whichever
	it is, each element is converted to its component's type and held to what
	the component stores, the fields that name the components are never stored,
	and a field that restates the key must agree with it. A key that cannot be
	built is refused with the component it lacks; no random key is drawn.
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

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# The key from SET and from CONTENT.
[[test.results]]
value = "[reading:['London', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['London', d'2024-01-02T00:00:00Z']]"

# An explicit id wins, and fields that restate it are accepted.
[[test.results]]
value = "[reading:['Paris', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "[reading:['Paris', d'2024-01-02T00:00:00Z']]"

# A field that disagrees with the explicit id is refused with the value written.
[[test.results]]
error = "Found 'Rome' for the `city` field, but it is part of the record id of `reading` and does not match it"

# A string is not a datetime.
[[test.results]]
match = "$error = /Couldn't coerce value for the record id component .time. of .reading.: /"
error = true

# An int component holds an int, whatever number was written.
[[test.results]]
value = "[counter:['a', 5]]"

[[test.results]]
value = "[counter:['b', 5]]"

[[test.results]]
value = "[true, true]"

[[test.results]]
match = "$error = /Couldn't coerce value for the record id component .n. of .counter.: /"
error = true

[[test.results]]
match = "$error = /Couldn't coerce value for the record id component .n. of .counter.: /"
error = true

# A datetime beyond the years a record id can order.
[[test.results]]
match = "$error = /Couldn't coerce value for the record id component .time. of .reading.: .*is a datetime outside the years 0000 to 9999/"
error = true

# The wrong number of elements.
[[test.results]]
error = "The record id of `reading` has the components [city, time], but 1 value was given"

[[test.results]]
error = "The record id of `reading` has the components [city, time], but 3 values were given"

# A component nothing supplies.
[[test.results]]
error = "Cannot create a record in `reading` without the record id component `time`: set `time`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
error = "Cannot create a record in `reading` without the record id component `city`: set `city`, give the record id, or define a DEFAULT on the `id` field"

# The DEFAULT is not evaluated while every component is written, and is
# evaluated when one is not.
[[test.results]]
value = "[sensor:['Rome', d'2024-02-01T00:00:00Z']]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[sensor:['site-2', d'2024-01-01T00:00:00Z']]"

# What was stored holds no component, and the refused writes stored nothing.
[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 4 }, { id: reading:['London', d'2024-01-02T00:00:00Z'], temp: 5 }, { id: reading:['Paris', d'2024-01-01T00:00:00Z'], temp: 6 }, { id: reading:['Paris', d'2024-01-02T00:00:00Z'], temp: 7 }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE TABLE counter SCHEMALESS;
DEFINE FIELD id ON counter TYPE [site: string, n: int];
DEFINE PARAM $default_raises VALUE true;
DEFINE TABLE sensor SCHEMALESS;
DEFINE FIELD id ON sensor TYPE [city: string, time: datetime] DEFAULT IF $default_raises { THROW 'the DEFAULT was evaluated' } ELSE { ['site-2', d'2024-01-01T00:00:00Z'] };

-- The key is made of the components the record is written with.
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 4 RETURN VALUE id;
CREATE reading CONTENT { city: 'London', time: d'2024-01-02T00:00:00Z', temp: 5 } RETURN VALUE id;

-- An explicit id names the record; a field restating it passes, a field
-- contradicting it is refused.
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 6 RETURN VALUE id;
CREATE reading:['Paris', d'2024-01-02T00:00:00Z'] SET city = 'Paris', time = d'2024-01-02T00:00:00Z', temp = 7 RETURN VALUE id;
CREATE reading:['Paris', d'2024-01-03T00:00:00Z'] SET city = 'Rome';

-- A string is not a datetime, so it is not converted to one.
CREATE reading SET city = 'London', time = '2024-01-03T00:00:00Z';

-- A float or a decimal that is a whole number becomes the int; another number
-- is refused rather than rounded, and one beyond the ints is not clamped.
CREATE counter SET site = 'a', n = 5.0 RETURN VALUE id;
CREATE counter SET site = 'b', n = 5dec RETURN VALUE id;
SELECT VALUE type::is_int(n) FROM counter;
CREATE counter SET site = 'c', n = 5.5;
CREATE counter SET site = 'c', n = 1e30;

-- 253402300800 seconds after the epoch is the first instant of the year 10000.
CREATE reading SET city = 'London', time = time::from_secs(253402300800);

-- The key has one element for each component.
CREATE reading:['London'] SET temp = 1;
CREATE reading:['London', d'2024-01-04T00:00:00Z', 1] SET temp = 1;

-- A component nothing supplies is named, in position order.
CREATE reading SET city = 'London', temp = 1;
CREATE reading SET temp = 1;

-- With every component written the DEFAULT is not evaluated, which it would
-- show by raising; with one missing it is.
CREATE sensor SET city = 'Rome', time = d'2024-02-01T00:00:00Z' RETURN VALUE id;
DEFINE PARAM OVERWRITE $default_raises VALUE false;
CREATE sensor SET temp = 3 RETURN VALUE id;

-- Retyping the id to one with no names shows the stored bodies as they are.
DEFINE FIELD OVERWRITE id ON reading TYPE [string, datetime];
SELECT * FROM reading ORDER BY temp;
