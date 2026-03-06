/**
[env]
auth = { level = "editor" }

[test]
reason = "Editor can execute all DML: SELECT, CREATE, UPDATE, DELETE, INSERT, UPSERT, RELATE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, name: 'alpha' }]"

[[test.results]]
value = "[{ id: item:2, name: 'beta' }]"

[[test.results]]
value = "[{ id: item:1, name: 'alpha' }, { id: item:2, name: 'beta' }]"

[[test.results]]
value = "[{ id: item:1, name: 'updated' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:3, name: 'gamma' }]"

[[test.results]]
value = "[{ id: item:4, name: 'delta' }]"

[[test.results]]
value = "[{ id: rel:ww0vfr98bzhz4q4ibms4, in: item:1, out: item:3 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: item:1, name: 'updated' }, { id: item:3, name: 'gamma' }, { id: item:4, name: 'delta' }]"

*/
DEFINE TABLE item;
DEFINE TABLE rel TYPE RELATION;
CREATE item:1 SET name = 'alpha';
CREATE item:2 SET name = 'beta';
SELECT * FROM item ORDER BY id;
UPDATE item:1 SET name = 'updated';
DELETE item:2;
INSERT INTO item { id: item:3, name: 'gamma' };
UPSERT item:4 SET name = 'delta';
RELATE item:1->rel->item:3;
SELECT * FROM item ORDER BY id;
