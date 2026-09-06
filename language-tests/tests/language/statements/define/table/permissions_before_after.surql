/**
[env]
namespace = true
database = true
imports = ["language/statements/define/table/permissions_before_after_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = "Table PERMISSIONS bind $before and $after for soft-delete patterns"

[[test.results]]
value = "[{ id: item:one, name: 'one' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:two, name: 'two' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:two, name: 'two' }]"

[[test.results]]
value = "[{ id: item:two, name: 'updated' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:two, name: 'updated' }]"

*/

-- Active rows are visible under FOR select WHERE deleted IS NONE.
SELECT * FROM item;

-- CREATE checks $after, skipped because $after.name is NONE
CREATE item:rejected SET name = NONE;

-- CREATE checks $after (new row), not $before.
CREATE item:two SET name = 'two';

-- UPDATE pre-check uses $before.deleted; post-check output is cut by SELECT permission.
UPDATE item:one SET deleted = time::now();

-- Soft-deleted rows are hidden from SELECT.
SELECT * FROM item;

-- Bulk UPDATE skips soft-deleted rows ($before.deleted IS NONE); only active rows are updated.
UPDATE item SET name = 'updated';

-- UPDATE denied once $before.deleted is set.
UPDATE item:one SET name = 'blocked';

-- Only the active, updated row remains visible.
SELECT * FROM item ORDER BY name;
