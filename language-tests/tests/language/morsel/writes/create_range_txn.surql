/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "a CREATE over a range of ids in an explicit transaction sees what an earlier statement of the transaction wrote, is seen by the next, and is undone with the transaction, as the serial run does, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1099 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1099 }]"

[[test.results]]
value = "[{ n: 2, total: 799 }, { n: 5, total: 300 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1099 }]"

*/

DEFINE TABLE c;
BEGIN;
CREATE |c:1..800| SET n = 1 RETURN NONE;
UPDATE c SET n = n + 1 WHERE n = 1 RETURN NONE;
CREATE |c:1000..1300| SET n = 5 RETURN NONE;
SELECT count() FROM c GROUP ALL;
COMMIT;
SELECT count() FROM c GROUP ALL;
SELECT n, count() AS total FROM c GROUP BY n ORDER BY n;
BEGIN;
CREATE |c:2000..2400| SET n = 9 RETURN NONE;
UPDATE c SET n = n + 1 WHERE n = 9 RETURN NONE;
CANCEL;
SELECT count() FROM c GROUP ALL;
