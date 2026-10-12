/**
[env]
namespace = true
database = true

[test]
reason = """
	The clauses that cannot reach a component by a name they do not spell do
	not block a change of the record id: `$value` in a field clause (the
	field's own value), session parameters, builtin methods and functions
	handed field values, a closure handed no record, a nested statement over
	another table, a graph lookup, an event that compares its record images or
	copies them whole. Renaming and removing the components are accepted.
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

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ doubled: [4, 6], len: 2, n: 1, note: 'hi', tags: [2, 3] }]"
*/

DEFINE TABLE other;
DEFINE TABLE w SCHEMALESS PERMISSIONS FOR select WHERE $auth.id = owner OR $session.ns = 'test' FOR create, update, delete FULL;
DEFINE FIELD id ON w TYPE [city: string, time: datetime];
DEFINE FIELD n ON w TYPE int ASSERT $value > 0;
DEFINE FIELD note ON w TYPE string VALUE string::lowercase($value);
DEFINE FIELD len ON w COMPUTED note.len();
DEFINE FIELD tags ON w TYPE array<int> VALUE $value[WHERE $this > 1];
DEFINE FIELD doubled ON w COMPUTED tags.map(|$t| $t * 2);
DEFINE FIELD others ON w COMPUTED (SELECT VALUE count() FROM other GROUP ALL);
DEFINE FIELD likes ON w COMPUTED $this->likes->post;
DEFINE EVENT changed ON w WHEN $before != $after THEN { CREATE log SET af = $after, bf = $before };
DEFINE EVENT copied ON w THEN { CREATE log CONTENT { copy: $value } };
CREATE w SET city = 'London', time = d'2024-01-01T00:00:00Z', n = 1, note = 'HI', tags = [1, 2, 3] RETURN NONE;
ALTER FIELD id ON w TYPE [place: string, time: datetime];
REMOVE FIELD id ON w;
SELECT n, note, len, tags, doubled FROM w;
