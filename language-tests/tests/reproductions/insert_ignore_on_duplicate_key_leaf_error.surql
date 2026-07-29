/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "INSERT IGNORE ... ON DUPLICATE KEY UPDATE skips a row whose create attempt fails, whichever layer raised the failure"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
match = "$error = /boom/"
error = true

[[test.results]]
value = "[]"
*/

-- A create failure raised outside the document layer, through a VALUE clause
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD n ON t VALUE (THROW "boom");

-- IGNORE skips the row even though the statement is retryable as an update
INSERT IGNORE INTO t (id) VALUES (t:1) ON DUPLICATE KEY UPDATE n = 1;

-- Without IGNORE the same failure is surfaced
INSERT INTO t (id) VALUES (t:2) ON DUPLICATE KEY UPDATE n = 1;

-- Neither statement stored a record
SELECT * FROM t;
