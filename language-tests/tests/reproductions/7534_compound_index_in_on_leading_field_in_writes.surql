/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7534
reason = """
`IN` on the leading field of a composite UNIQUE index names a prefix of the
index's columns, which can match any number of entries. Each value is read as
a prefix scan, as equality on the same field is, so `SELECT`, `UPDATE` and
`DELETE` with the same `WHERE` all reach every row whose leading field is in
the list. A value that names no entry matches nothing, and `NONE` in the list
matches a row with no value for the field. The index still enforces
uniqueness on the whole tuple.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ file: 'f1', id: item:a, n: 1, owner: owner:o }]"

[[test.results]]
value = "[{ file: 'f2', id: item:b, n: 1, owner: owner:o }]"

[[test.results]]
value = "[{ file: 'f1', id: item:c, n: 1, owner: owner:p }]"

[[test.results]]
value = "[{ file: 'f1', id: item:d, n: 1, owner: owner:q }]"

[[test.results]]
value = "[{ file: 'f1', id: item:e, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:a, item:b, item:c]"

[[test.results]]
value = "[item:d]"

[[test.results]]
value = "[item:d, item:e]"

[[test.results]]
value = "[item:a, item:b, item:c]"

[[test.results]]
value = "[[item:a, 2], [item:b, 2], [item:c, 2], [item:d, 1], [item:e, 1]]"

[[test.results]]
value = "[{ file: 'f1', id: item:a, n: 2, owner: owner:o }, { file: 'f2', id: item:b, n: 2, owner: owner:o }, { file: 'f1', id: item:c, n: 2, owner: owner:p }]"

[[test.results]]
value = "[item:d, item:e]"

[[test.results]]
value = "[{ file: 'f1', id: item:f, owner: owner:o }]"

[[test.results]]
error = "Database index `item_owner_file` already contains [owner:o, 'f1'], with record `item:f`"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE INDEX item_owner_file ON item FIELDS owner, file UNIQUE;
CREATE item:a SET owner = owner:o, file = 'f1', n = 1;
CREATE item:b SET owner = owner:o, file = 'f2', n = 1;
CREATE item:c SET owner = owner:p, file = 'f1', n = 1;
CREATE item:d SET owner = owner:q, file = 'f1', n = 1;
CREATE item:e SET file = 'f1', n = 1;
-- `owner:missing` names no entry; `item:e` has no owner.
LET $l = [owner:o, owner:p, owner:missing];
SELECT VALUE id FROM item WHERE owner IN $l ORDER BY id;
SELECT VALUE id FROM item WHERE owner IN [owner:q] ORDER BY id;
SELECT VALUE id FROM item WHERE owner IN [NONE, owner:q] ORDER BY id;
UPDATE item SET n = 2 WHERE owner IN $l RETURN VALUE id;
SELECT VALUE [id, n] FROM item ORDER BY id;
DELETE item WHERE owner IN $l RETURN BEFORE;
SELECT VALUE id FROM item ORDER BY id;

-- The index still enforces uniqueness on the whole tuple.
CREATE item:f SET owner = owner:o, file = 'f1';
CREATE item:g SET owner = owner:o, file = 'f1';
