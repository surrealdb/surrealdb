/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "By design: a UNIQUE index stores a tuple holding NONE or NULL as a plain entry that names its record, so rows that differ only by a NONE member coexist, as rows holding NULL do in SQL"
issue = 7128

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
value = "[{ i: 1, id: t:2 }]"

[[test.results]]
value = "[{ i: 1, id: t:1 }, { i: 1, id: t:2 }]"
*/

DEFINE TABLE t SCHEMAFUL;
DEFINE FIELD i ON t TYPE int;
DEFINE FIELD j ON t TYPE option<string>;
DEFINE INDEX t_idx ON t FIELDS i, j UNIQUE;
CREATE t:1 SET i = 1;
CREATE t:2 SET i = 1;
SELECT * FROM t;
