/**
[env]
namespace = true
database = true

[test]
reason = """
	A record body that stores a component's name with the value the record id
	holds there is a copy of it, not a conflict: the structure can be named, and
	the derived field takes over from the copy. A copy of the other component,
	a field that is absent and a body with other fields all pass the same way.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2, 3]"
*/

CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET city = 'London', temp = 1 RETURN NONE;
CREATE reading:['London', d'2024-01-02T00:00:00Z'] SET time = d'2024-01-02T00:00:00Z', city = 'London', temp = 2 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 3 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

SELECT VALUE temp FROM reading ORDER BY temp;
