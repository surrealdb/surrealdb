/**
[env]
namespace = true
database = true
imports = ["access/record/create_permission_vs_data_clause_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
The pre-write permission check on a creating statement is not the same check as on
an updating one. `PERMISSIONS FOR create NONE` short-circuits before the data clause
is evaluated, so a data clause with side effects does not run. A *conditional* create
permission is evaluated only after the data clause has already run, so its side
effects do happen even though the write is refused. Pinned because a single
pre-write permission stage that evaluated the condition in full would change this.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "1"
*/

RETURN $auth;
-- FOR create NONE: refused before the data clause, so no `log` record.
CREATE pnone:1 SET spawn = (CREATE log).id;
-- FOR create WHERE false: refused after the data clause, so one `log` record exists.
CREATE pcond:1 SET spawn = (CREATE log).id;
SELECT * FROM pcond;
RETURN array::len((SELECT id FROM log));
