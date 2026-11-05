/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "INSERT IGNORE ... ON DUPLICATE KEY UPDATE skips only a conflict: a create attempt failing in a VALUE clause is raised, with or without IGNORE"
issue = 7564

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$error = /boom/"
error = true

[[test.results]]
match = "$error = /boom/"
error = true

[[test.results]]
value = "[]"
*/

-- A create failure raised outside the document layer, through a VALUE clause
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD n ON t VALUE (THROW "boom");

-- A VALUE failure is not a conflict, so IGNORE does not skip it
INSERT IGNORE INTO t (id) VALUES (t:1) ON DUPLICATE KEY UPDATE n = 1;

-- Nor is it skipped without IGNORE
INSERT INTO t (id) VALUES (t:2) ON DUPLICATE KEY UPDATE n = 1;

-- Neither statement stored a record
SELECT * FROM t;
