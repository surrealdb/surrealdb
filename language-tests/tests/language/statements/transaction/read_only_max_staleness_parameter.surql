/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "MAX STALENESS takes a duration parameter bound outside the block; a parameter that is not a duration, or is not defined, fails the block."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:one, value: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The parameter $n used for MAX STALENESS is not a duration"

[[test.results]]
error = "Cannot COMMIT: The parameter $n used for MAX STALENESS is not a duration"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The parameter $missing used for MAX STALENESS is not defined"

[[test.results]]
error = "Cannot COMMIT: The parameter $missing used for MAX STALENESS is not defined"
*/
DEFINE TABLE item SCHEMALESS;
CREATE item:one SET value = 1;

LET $s = 5s;
BEGIN READ ONLY MAX STALENESS $s;
SELECT VALUE value FROM item;
COMMIT;

LET $n = 5;
BEGIN READ ONLY MAX STALENESS $n;
SELECT VALUE value FROM item;
COMMIT;

BEGIN READ ONLY MAX STALENESS $missing;
SELECT VALUE value FROM item;
COMMIT;
