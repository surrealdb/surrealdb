/**
[env]
imports = ["reproductions/a_write_output_select_permission_reads_computed_fields_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "The output of a write is judged by the table's `PERMISSIONS FOR select` with the COMPUTED fields it reads derived from the image the statement shows: the pre-image of a `DELETE ... RETURN BEFORE`, the post-image of an update. A COMPUTED field is not stored, so judged against the image as written the predicate reads `NONE` — a deny-intended clause shows every record, an allow-intended one shows none."

# The pre-image is hidden; the delete itself is permitted.
[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:pub]"

# The post-image decides: an update that makes a record public shows it, one
# that makes it secret does not.
[[test.results]]
value = "[{ id: doc:sec2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: page:pub }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:sec2]"

[[test.results]]
value = "[page:pub]"

*/

DELETE doc:sec RETURN BEFORE;
DELETE doc:pub RETURN VALUE $before.id;

UPDATE doc:sec2 SET status = 'public' RETURN id;
UPDATE doc:pub2 SET status = 'secret' RETURN id;

UPDATE page:pub SET n = 1 RETURN id;
UPDATE page:sec SET n = 1 RETURN id;

SELECT VALUE id FROM doc ORDER BY id;
SELECT VALUE id FROM page ORDER BY id;
