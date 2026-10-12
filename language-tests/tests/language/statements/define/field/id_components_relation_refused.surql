/**
[env]
namespace = true
database = true

[test]
reason = """
	The record id of a relation table is its `in`, its `out` and its own key,
	so it has no components to name. A plain relation and a lightweight one
	refuse alike, whether the id is defined or altered; a table that is not a
	relation names its components.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name the parts of the record id of `follows`: it is a relation table"

[[test.results]]
error = "Cannot name the parts of the record id of `likes`: it is a relation table"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name the parts of the record id of `follows`: it is a relation table"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE person;
DEFINE TABLE follows TYPE RELATION IN person OUT person;
DEFINE TABLE likes TYPE RELATION IN person OUT person LIGHTWEIGHT;

-- A relation table, plain or lightweight.
DEFINE FIELD id ON follows TYPE [city: string, time: datetime];
DEFINE FIELD id ON likes TYPE [city: string, time: datetime];

-- ALTER FIELD refuses the same way.
DEFINE FIELD id ON follows TYPE string;
ALTER FIELD id ON follows TYPE [city: string, time: datetime];

-- A table that is not a relation names its components.
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
