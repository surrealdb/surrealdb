/**
[env]
imports = ["reproductions/an_upsert_of_a_stored_record_is_judged_by_its_update_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }

[test]
reason = """
An UPSERT that names a stored record is an update of it, with or without a
WHERE: the update permission judges the record before the data clause is
evaluated, and the create permission plays no part. A caller the update
permission refuses is told nothing, whatever the clause would have raised
against an empty record (an id that is not the record's, `n + 1` with `n`
absent). A caller who may update and not create updates the stored record. A
record that is not stored is a create, judged by the create permission.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 2 }]"

[[test.results]]
value = "[{ n: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: locked:3, n: 1 }]"

[[test.results]]
value = "[{ id: locked:1, n: 1 }, { id: locked:3, n: 1 }, { id: update_only:1, n: 3 }]"
*/

UPSERT locked:1 SET id = locked:2;
UPSERT locked:1 SET n = n + 1;
UPSERT locked:1 SET n = n + 1 WHERE true;
UPSERT update_only:1 SET n = n + 1 RETURN n;
UPSERT update_only:1 SET n = n + 1 WHERE true RETURN n;
UPSERT update_only:2 SET n = 1;
UPSERT locked:3 SET n = 1;
SELECT * FROM locked, update_only ORDER BY id;
