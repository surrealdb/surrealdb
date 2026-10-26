/**
[env]
namespace = true
database = true
imports = ["language/statements/insert/insert_select_permission_skip_keeps_writes_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
A record that is written and then withheld from the caller by its select
permission is kept, with what its event wrote: the skip only withholds the
output. Holds for INSERT, UPSERT and an ON DUPLICATE KEY UPDATE update.

It holds as well for an UPSERT whose create attempt fails a schema check and is
retried as an update of the record it names: the update stores the record, so
the statement succeeds and the create attempt's error is dropped, however the
output is withheld afterwards. Where the record does not exist, the update has
nothing to store and the create attempt's error is raised.
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
value = "['CREATE', 'CREATE', 'UPDATE']"

[[test.results]]
value = "[]"

[[test.results]]
error = "Couldn't coerce value for field `req` of `strict:2`: Expected `int` but found `NONE`"

[[test.results]]
value = "[['CREATE', 1], ['UPDATE', 2]]"

*/

RETURN $auth;
INSERT INTO hidden { id: hidden:1 };
UPSERT hidden:2 SET v = 1;
INSERT INTO hidden { id: hidden:1 } ON DUPLICATE KEY UPDATE v = 2;
SELECT VALUE at FROM seen ORDER BY at;

-- The create attempt fails on `req`, which the stored record holds
UPSERT strict SET id = 1, v = 2;
-- No record to update: the create attempt's error is raised
UPSERT strict SET id = 2, v = 3;
SELECT VALUE [at, v] FROM strict_seen ORDER BY at;
