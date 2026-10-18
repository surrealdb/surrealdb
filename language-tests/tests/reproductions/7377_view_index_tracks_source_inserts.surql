/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An index defined on a pre-computed view reflects rows added to the source table after the index was created"
issue = 7377

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[3, 2, 1]"

[[test.results]]
value = "[1, 2, 3]"
*/

CREATE parent:1 CONTENT { addedTime: time::now() } RETURN NONE;
CREATE parent:2 CONTENT { addedTime: time::now() + 1s } RETURN NONE;
DEFINE TABLE child TYPE NORMAL SCHEMALESS AS SELECT * FROM parent PERMISSIONS NONE;
DEFINE INDEX child__addedTime ON child FIELDS addedTime;
CREATE parent:3 CONTENT { addedTime: time::now() + 4s } RETURN NONE;
SELECT VALUE meta::id(id) FROM child ORDER BY addedTime DESC;
SELECT VALUE meta::id(id) FROM child ORDER BY addedTime ASC;
