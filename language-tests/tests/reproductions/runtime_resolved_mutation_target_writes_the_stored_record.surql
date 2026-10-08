/**
[env]
namespace = true
database = true

[test]
reason = """
A mutation whose target becomes a value only when the statement runs — a
function call, or a closure applied to its arguments — writes the stored record
the value names, not the value itself. An object carrying an `id` is the record
that `id` names, so a statement leaves every field it did not name in place,
and an id no record answers is no subject at all rather than a record to
invent.

A read of the same expression answers differently: it yields the object as the
row it is, and a target resolving to a table name is filtered by the reader's
SELECT permission rather than gated by the statement's own clause.

Every function here reads the database, which is what makes the test bite. A
function whose body is constant is folded to its value before the planner sees
it, so the mutation names a written value and is classified with the targets —
a path that already reads the stored record, and would report this working
whether it was or not.
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
value = "[{ id: setting:1, kind: 'person' }]"

[[test.results]]
value = "[{ age: 30, id: person:1, name: 'a' }]"

[[test.results]]
value = "[{ age: 31, id: person:2, name: 'a' }]"

[[test.results]]
value = "[{ age: 32, id: person:3, name: 'a' }]"

[[test.results]]
value = "[{ age: 30, id: person:1, name: 'b' }]"

[[test.results]]
value = "[{ age: 30, id: person:1, name: 'b' }]"

[[test.results]]
value = "[{ age: 31, id: person:2, name: 'b' }]"

[[test.results]]
value = "[{ age: 31, id: person:2, name: 'b' }]"

[[test.results]]
value = "[{ age: 30, id: person:1, name: 'b', seen: true }, { age: 31, id: person:2, name: 'b', seen: true }, { age: 32, id: person:3, name: 'a', seen: true }]"

[[test.results]]
value = "[{ age: 30, id: person:1, name: 'b', seen: true }, { age: 31, id: person:2, name: 'b', seen: true }, { age: 32, id: person:3, name: 'a', seen: true }]"

[[test.results]]
error = "Cannot execute UPDATE statement using value: NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE FUNCTION fn::projected() { RETURN (SELECT id, name FROM person:1)[0]; };
DEFINE FUNCTION fn::named() { RETURN (SELECT VALUE id FROM person:2)[0]; };
DEFINE FUNCTION fn::absent() { RETURN (SELECT VALUE missing FROM person:2)[0]; };
DEFINE FUNCTION fn::table_name() { RETURN type::table((SELECT VALUE kind FROM setting:1)[0]); };
DEFINE FUNCTION fn::doomed() { RETURN (SELECT id, name FROM person:3)[0]; };

CREATE setting:1 SET kind = 'person';
CREATE person:1 SET name = 'a', age = 30;
CREATE person:2 SET name = 'a', age = 31;
CREATE person:3 SET name = 'a', age = 32;

-- A call answering with an object that carries the id, projected so that it
-- omits a stored field. The subject is the record the id names, so `age`
-- survives a statement that never mentioned it.
UPDATE fn::projected() SET name = 'b';
SELECT * FROM person:1;

-- A call naming the record outright.
UPDATE fn::named() SET name = 'b';
SELECT * FROM person:2;

-- A target naming the table only once the statement runs. The records are
-- the statement's subjects, gated by its own clause, and not rows a reader
-- selected.
UPDATE fn::table_name() SET seen = true;
SELECT id, name, age, seen FROM person ORDER BY id;

-- An id no record answers is no subject.
UPDATE fn::absent() SET name = 'b';

-- The same shape is the subject a DELETE removes: the stored record, not the
-- projection that named it.
DELETE fn::doomed();
SELECT * FROM person:3;
