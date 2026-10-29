/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An indexed UPDATE must process a record once even when the updated index entry reappears in the scan"
issue = 7641

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:a, status: 'pending', attempts: 0 }]"

[[test.results]]
value = "[{ id: t:b, status: 'resolved', attempts: 0 }]"

[[test.results]]
value = "[{ id: t:a, status: 'pending', next_attempt_at: d'2100-01-01T00:00:00Z', attempts: 1 }]"

[[test.results]]
value = "[{ id: t:a, attempts: 1 }, { id: t:b, attempts: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:a, n: 1, attempts: 0 }]"

[[test.results]]
value = "[{ id: r:a, n: 2, attempts: 1 }]"

[[test.results]]
value = "[{ id: r:a, n: 2, attempts: 1 }]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX t_due ON t FIELDS status, next_attempt_at;
CREATE t:a SET status = 'pending', next_attempt_at = NONE, attempts = 0;
CREATE t:b SET status = 'resolved', next_attempt_at = NONE, attempts = 0;

UPDATE t SET next_attempt_at = d'2100-01-01T00:00:00Z', attempts = attempts + 1
    WHERE id IN [t:a, t:b] AND status = 'pending'
    RETURN AFTER;
SELECT id, attempts FROM t ORDER BY id;

DEFINE TABLE r SCHEMALESS;
DEFINE INDEX r_n ON r FIELDS n;
CREATE r:a SET n = 1, attempts = 0;
UPDATE r SET n = n + 1, attempts = attempts + 1 WHERE n >= 1 AND n < 3 RETURN AFTER;
SELECT id, n, attempts FROM r;
