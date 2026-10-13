/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A COMPUTED field that reads another COMPUTED field is derived after it, whatever the two are named. The streaming read path orders the fields by dependency; the document pipeline — the legacy read path, every write's output, and the values a write permission reads — must use the same order. Here each field reads one that sorts after it by name, so name order derives `a` from `b = NONE` and `can_edit` from `is_locked = NONE`."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# A write's output is derived by the document pipeline on every strategy.
[[test.results]]
value = "[{ a: 100, b: 10, c: 1, can_edit: false, id: t:1, is_locked: true, n: 1, status: 'locked' }]"

[[test.results]]
value = "[{ a: 200, b: 20, c: 2, can_edit: false, id: t:1, is_locked: true, n: 2, status: 'locked' }]"

# The read path, projected and whole.
[[test.results]]
value = "[{ a: 200, can_edit: false }]"

[[test.results]]
value = "[{ a: 200, b: 20, c: 2, can_edit: false, id: t:1, is_locked: true, n: 2, status: 'locked' }]"

[[test.results]]
value = "[false]"

*/

DEFINE FIELD a ON t COMPUTED b * 10;
DEFINE FIELD b ON t COMPUTED c * 10;
DEFINE FIELD c ON t COMPUTED n;
DEFINE FIELD is_locked ON t COMPUTED status = 'locked';
DEFINE FIELD can_edit ON t COMPUTED !is_locked;

CREATE t:1 SET n = 1, status = 'locked';
UPDATE t:1 SET n = 2;

SELECT a, can_edit FROM t:1;
SELECT * FROM t:1;
SELECT VALUE can_edit FROM t WHERE can_edit = false;
