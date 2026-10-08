/**
[env]
namespace = true
database = true

[test]
reason = """
A mutation written where a scalar is expected reads the row that holds it, so a
subject named by a bare path resolves against that row rather than against no
document: `SELECT (UPSERT link.target SET n += 1) FROM cfg:1` writes the record
that `cfg:1`'s `link.target` names, whether the path steps through an object
field or dereferences a record link, and an `INSERT` whose value reads a field
of the enclosing row stores that field's value.

A statement with no enclosing row resolves the same path against no document,
so it names no subject and the form refuses the target.
"""

[[test.results]]
value = "[{ id: counter:direct, n: 0 }]"

[[test.results]]
value = "[{ id: counter:linked, n: 0 }]"

[[test.results]]
value = "[{ id: holder:1, target: counter:linked }]"

[[test.results]]
value = "[{ id: cfg:1, link: holder:1, nested: { target: counter:direct } }]"

[[test.results]]
value = "[{ bumped: [{ id: counter:direct, n: 1 }] }]"

[[test.results]]
value = "[{ bumped: [{ id: counter:linked, n: 1 }] }]"

[[test.results]]
value = "[1, 1]"

[[test.results]]
value = "[{ written: [{ counted: counter:direct, id: note:a }] }]"

[[test.results]]
value = "[counter:direct]"

[[test.results]]
error = "Cannot execute UPSERT statement using value: NONE"
*/

CREATE counter:direct SET n = 0;
CREATE counter:linked SET n = 0;
CREATE holder:1 SET target = counter:linked;
CREATE cfg:1 SET nested = { target: counter:direct }, link = holder:1;

-- A path into an object field of the enclosing row.
SELECT (UPSERT nested.target SET n += 1) AS bumped FROM cfg:1;

-- A path that dereferences a record link on the enclosing row.
SELECT (UPSERT link.target SET n += 1) AS bumped FROM cfg:1;

SELECT VALUE n FROM counter ORDER BY id;

-- An `INSERT` value reads the enclosing row the same way.
SELECT (INSERT INTO note { id: 'a', counted: nested.target }) AS written FROM cfg:1;
SELECT VALUE counted FROM note;

-- With no enclosing row the path resolves against no document, so the
-- statement names no subject.
UPSERT nested.target SET n += 1;
