/**
[env]
auth = { level = "viewer" }
imports = ["language/permissions/dml/viewer_write_denied_import.surql"]

[test]
reason = "Viewer write DML is silently denied: CREATE, UPDATE, DELETE, INSERT, UPSERT, RELATE all return empty"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:1, name: 'alpha' }, { id: item:2, name: 'beta' }]"

*/
CREATE item:99 SET name = 'nope';
UPDATE item:1 SET name = 'nope';
DELETE item:1;
INSERT INTO item { id: item:98, name: 'nope' };
UPSERT item:1 SET name = 'nope';
RELATE item:1->rel->item:2;
-- Verify original data is unchanged
SELECT * FROM item ORDER BY id;
