/**
[env]
namespace = true
database = true

[test]
reason = """
	Pins which error a caller sees when a write to a table whose `id` names its
	components is wrong in two ways at once, as `data_clause_error_precedence`
	does for the special fields. The key is settled after the data clause is
	applied and the record's `id` is checked, and before the table's own fields
	are processed. So a data clause that fails to apply is reported before
	anything about the key; a contradicted `id` is reported before a
	contradicted component; and a key that cannot be made, or a component that
	contradicts it, is reported before a field's ASSERT. The last write that
	fails shows the ASSERT is live, so the ones above it hide a failure that
	exists.
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
value = "[]"

# The data clause fails to apply: reported first, on a create and on an update.
[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"

[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"

# A contradicted `id` is reported before a contradicted component.
[[test.results]]
error = "Found reading:['Paris', d'2024-01-01T00:00:00Z'] for the `id` field, but a specific record has been specified"

[[test.results]]
error = "Found reading:['Paris', d'2024-01-01T00:00:00Z'] for the `id` field, but a specific record has been specified"

# A contradicted component is reported before a field's ASSERT.
[[test.results]]
error = "Found 'Rome' for the `city` field, but it is part of the record id of `reading` and does not match it"

# A key that cannot be made is reported before a field's ASSERT, and a key with
# the wrong number of elements before a contradicted component.
[[test.results]]
error = "Cannot create a record in `reading` without the record id component `time`: set `time`, give the record id, or define a DEFAULT on the `id` field"

[[test.results]]
error = "The record id of `reading` has the components [city, time], but 1 value was given"

# The data clause is applied before the key is made.
[[test.results]]
error = "Cannot perform subtraction with 'int' and 'string'"

# The ASSERT is live: with nothing else wrong it is what is reported.
[[test.results]]
match = "$error = /Found -1 for field .temp., with record .*, but field must conform to: /"
error = true

# None of the refused writes changed the record.
[[test.results]]
value = "[{ n: 1, temp: 1 }]"
*/

DEFINE TABLE reading SCHEMAFULL;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE int ASSERT $value > 0;
DEFINE FIELD n ON reading TYPE int;
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 1, n = 1 RETURN NONE;

-- The data clause fails to apply, and also contradicts the key.
CREATE reading:['London', d'2024-01-02T00:00:00Z'] SET city = 'Rome', temp = 1, n = 1, n -= 'abc';
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET city = 'Rome', n = 1, n -= 'abc';

-- The `id` and a component are both contradicted.
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET id = reading:['Paris', d'2024-01-01T00:00:00Z'], city = 'Rome';
CREATE reading:['London', d'2024-01-03T00:00:00Z'] SET id = reading:['Paris', d'2024-01-01T00:00:00Z'], city = 'Rome', temp = 1, n = 1;

-- A component is contradicted and a field's ASSERT fails.
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET city = 'Rome', temp = -1;

-- A component is missing and a field's ASSERT fails; a key is short and a
-- component is contradicted.
CREATE reading SET city = 'London', temp = -1, n = 1;
CREATE reading:['London'] SET city = 'Rome', temp = 1, n = 1;

-- The data clause fails to apply and a component is missing.
CREATE reading SET temp = 1, n = 1, n -= 'abc';

-- With nothing else wrong, the ASSERT is what fails.
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = -1;

SELECT temp, n FROM reading;
