/**
[env]
namespace = true
database = true

[test]
reason = """
A create inside BEGIN ... COMMIT is checked against the transaction's own writes when its check is
made for many creates at once: a record the transaction deleted may be created again, a record it
created may not, a record stored before the transaction fails its create at that statement, and a
failed create leaves the stored record as it was."""

[[test.results]]
value = "[{ id: d:1, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: d:1, v: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: d:1, v: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Database record `d:2` already exists"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ id: d:1, v: 2 }]"

[[test.results]]
value = "[{ id: x:1, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database record `x:1` already exists"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ id: x:1, v: 1 }]"

[[test.results]]
value = "[{ id: x:1, v: 1 }]"
*/
CREATE d:1 SET v = 1;
BEGIN;
DELETE d:1;
CREATE d:1 SET v = 2;
COMMIT;
SELECT * FROM d;
BEGIN;
CREATE d:2;
CREATE d:2;
COMMIT;
SELECT * FROM d;
CREATE x:1 SET v = 1;
BEGIN;
CREATE |x:1..3| SET v = 2;
COMMIT;
SELECT * FROM x ORDER BY id;
SELECT * FROM x:1;
