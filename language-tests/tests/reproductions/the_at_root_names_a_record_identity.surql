/**
[env]
namespace = true
database = true

[test]
reason = """
`@` names the identity of the record a statement is iterating, not a position
inside the row. A row is a record when it is the record itself — an object
whose `id` is a record id — or the bare record id; any other row is a value
with no identity, and an `id` key it happens to hold is ordinary user data.
Reading that key as identity would expose whatever the row put there: a
string, an object, or a nested array's per-element ids.

A record reached through a literal array is still a record: the source
dereferences the id, so the row it yields carries its identity. What decides
is what the row is, not how it was written.

`@` is a root and never a whole projection — the parser requires a step after
it — so each query here continues the path, and `@.a` reads a field off the
identity by dereferencing it.
"""

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ id: e:1, in: t:1, out: t:2 }]"

[[test.results]]
value = "['t:1', 't:2']"

[[test.results]]
value = "['t:1', 't:2']"

[[test.results]]
value = "['t:2']"

[[test.results]]
value = "['e:1']"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "['t:1']"

[[test.results]]
value = "['NONE']"

[[test.results]]
value = "['NONE']"

[[test.results]]
value = "['NONE']"

[[test.results]]
value = "['NONE']"

[[test.results]]
value = "['NONE']"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "['t:1', 'NONE']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "'NONE'"

[[test.results]]
value = "[{ seen: 't:1' }]"

*/

CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
RELATE t:1->e:1->t:2;

-- Every source that yields records yields their identity.
SELECT VALUE @.to_string() FROM t;
SELECT VALUE @.to_string() FROM t:1..=2;
SELECT VALUE @.to_string() FROM t:1->e->t;
SELECT VALUE @.to_string() FROM e;

-- The identity is a record id, so the path continues by dereferencing it.
SELECT VALUE @.a FROM t:1;
SELECT VALUE @.id FROM t:1;

-- A record id written into a literal array is dereferenced by the source, so
-- the row is a record and carries its identity.
SELECT VALUE @.to_string() FROM [t:1];

-- A row that is not a record has no identity, whatever its `id` key holds.
SELECT VALUE @.to_string() FROM [{ id: 'fake', a: 1 }];
SELECT VALUE @.to_string() FROM [{ id: { k: 1 }, a: 1 }];
SELECT VALUE @.to_string() FROM [{ a: 1 }];
SELECT VALUE @.to_string() FROM 7;
SELECT VALUE @.to_string() FROM (SELECT VALUE @.to_string() FROM [{ id: 'inner' }])[0];

-- And a step off an absent identity keeps descending rather than reading the
-- row's own field.
SELECT VALUE @.a FROM [{ id: 'fake', a: 1 }];

-- The two kinds of row can sit in one source.
SELECT VALUE @.to_string() FROM [t:1, { id: 'fake' }];

-- A predicate reads the same identity: the literal row matches nothing.
SELECT * FROM [{ id: 'fake', a: 1 }] WHERE @.to_string() = 'fake';
SELECT * FROM t:1 WHERE @.to_string() = 't:1';

-- With no row at all there is no identity.
RETURN @.to_string();

-- A mutation iterates the record it is writing.
UPDATE t:1 SET seen = @.to_string() RETURN seen;