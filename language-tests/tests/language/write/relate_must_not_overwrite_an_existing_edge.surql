/**
[env]
namespace = true
database = true

[test]
reason = """
A RELATE that resolved to a new edge must not overwrite a record that already
exists at the id its data clause names.

`Document::relate` picks the create path from the loaded record being nullish,
and it picks it BEFORE the SET / CONTENT / MERGE clause runs. So a clause that
assigns `id` moves the write to an id the dispatch decision never saw. The
store stage therefore keys its create-only write on the initial image rather
than on that decision: keyed on the decision instead, an assigned
`id = e:existing` reaches the overwriting write and replaces a pre-existing
edge under create permissions alone.

The two are indistinguishable until the ids differ, which is why this is
written down: every other RELATE test passes either way.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ id: e:existing, in: a:1, out: b:1, tag: 'first' }]"

[[test.results]]
error = "Database record `e:existing` already exists"

[[test.results]]
value = "[{ id: e:existing, in: a:1, out: b:1, tag: 'first' }]"
*/

DEFINE TABLE e SCHEMALESS PERMISSIONS FOR create FULL, FOR select FULL, FOR update NONE;

CREATE a:1;
CREATE b:1;

-- Takes the create path and lands on the id the clause names.
RELATE a:1->e->b:1 SET id = e:existing, tag = 'first';

-- Takes the create path too: the edge id is generated, so nothing is loaded
-- and the record is nullish. The clause then names an id that does exist.
RELATE a:1->e->b:1 SET id = e:existing, tag = 'second';

-- The first edge is intact: the second RELATE wrote nothing.
SELECT * FROM e;
