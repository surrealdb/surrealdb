/**
[env]
namespace = true
database = true
auth = { namespace = "test", database = "test", level = "viewer" }

[test]
reason = "A principal without the right to DEFINE TABLE must not define one as a side effect of a write. CREATE, UPSERT, INSERT and RELATE name their table and define it on demand in a non-STRICT database; the on-demand definition needs the same authority DEFINE TABLE does."
issue = 7600

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

# None of the statements above left a table behind.
[[test.results]]
value = "[]"
*/

CREATE top_create:1;
UPSERT top_upsert:1;
INSERT INTO top_insert { id: 1 };
RELATE a:1->top_relate->b:2;
SELECT * FROM (CREATE sub_create:1);
SELECT * FROM (RELATE a:1->sub_relate->b:2);
RETURN object::keys((INFO FOR DB).tables);
