/**
[env]
namespace = true
database = true

[test]
reason = """
	A record body that stores a field under the name of a component, with a
	value other than the component's, would be shadowed by the derived field,
	so naming the component is refused and the error gives the remedy. Once the
	field is unset the structure can be named.
"""

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the record reading:['London', d'2024-01-01T00:00:00Z'] stores a `city` field that differs from its record id. Remove it first, for example with UPDATE reading UNSET city"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"
*/

CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET city = 'Paris', temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- The remedy the error names.
UPDATE reading UNSET city RETURN NONE;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
