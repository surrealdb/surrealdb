/**
[env]
namespace = true
database = true

[test]
reason = """
	A view that selects every field but holds no row has no row written under
	another record id, so naming the components of its source is accepted, and
	every row it holds afterwards carries them. Once it holds a row, removing
	the names is refused; emptying the view's source leaves no row, so the
	names can be removed again.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'London', id: v:['London', d'2024-01-01T00:00:00Z'], temp: 10, time: d'2024-01-01T00:00:00Z' }, { city: 'Paris', id: v:['Paris', d'2024-01-02T00:00:00Z'], temp: 20, time: d'2024-01-02T00:00:00Z' }]"

[[test.results]]
value = "[{ city: 'Paris', id: v:['Paris', d'2024-01-02T00:00:00Z'], temp: 20, time: d'2024-01-02T00:00:00Z' }]"

[[test.results]]
error = "Cannot change the record id of `reading`: the view `v` selects every field of `reading` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE TABLE v AS SELECT * FROM reading;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 10 RETURN NONE;
CREATE reading SET city = 'Paris', time = d'2024-01-02T00:00:00Z', temp = 20 RETURN NONE;
SELECT * FROM v ORDER BY temp;
SELECT * FROM v WHERE city = 'Paris';

REMOVE FIELD id ON reading;
DELETE reading;
SELECT * FROM v;
REMOVE FIELD id ON reading;
