/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A compound UNIQUE index treats a NONE member as a value, so two rows with the same other member and NONE collide"
issue = 7128
wip = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: 1, id: t:1 }]"

[[test.results]]
error = "Database index `t_idx` already contains [1, NONE], with record `t:1`"

[[test.results]]
value = "[{ i: 1, id: t:1 }]"
*/

DEFINE TABLE t SCHEMAFUL;
DEFINE FIELD i ON t TYPE int;
DEFINE FIELD j ON t TYPE option<string>;
DEFINE INDEX t_idx ON t FIELDS i, j UNIQUE;
CREATE t:1 SET i = 1;
CREATE t:2 SET i = 1;
SELECT * FROM t;
