/**
[env]
namespace = true
database = true

[test]
reason = """
`INSERT IGNORE` skips a row only on a conflict: a duplicate record id or a
unique-index entry. Generating the row's record id is part of its create
attempt, and an id the `id` field's declared kind rejects is no conflict: it
fails the statement, with or without IGNORE, and none of the batch is written.
The same holds for an id the field's DEFAULT generates. A duplicate id in the
same kind of batch is still skipped.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `id` of `gid:nope`: Expected `int` but found `'nope'`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Couldn't coerce value for field `id` of `gid:nope`: Expected `int` but found `'nope'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `id` of `dt:bad`: Expected `int` but found `'bad'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[gid:2]"

[[test.results]]
value = "[gid:1, gid:2]"
*/

DEFINE TABLE gid SCHEMALESS;
DEFINE FIELD id ON gid TYPE int;
INSERT IGNORE INTO gid [{ id: 1 }, { id: 'nope' }, { id: 3 }] RETURN VALUE id;
SELECT VALUE id FROM gid ORDER BY id;
INSERT INTO gid [{ id: 4 }, { id: 'nope' }];
SELECT VALUE id FROM gid ORDER BY id;

-- An id DEFAULT that yields an id the kind rejects
DEFINE TABLE dt SCHEMALESS;
DEFINE FIELD id ON dt TYPE int DEFAULT 'bad';
INSERT IGNORE INTO dt [{ v: 1 }, { v: 2 }] RETURN VALUE id;
SELECT VALUE id FROM dt;

-- A duplicate id is skipped
INSERT INTO gid { id: 1 } RETURN NONE;
INSERT IGNORE INTO gid [{ id: 1 }, { id: 2 }] RETURN VALUE id;
SELECT VALUE id FROM gid ORDER BY id;
