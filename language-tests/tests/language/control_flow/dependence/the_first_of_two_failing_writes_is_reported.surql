/**
[test]
reason = "Of two failing writes to different tables in one block, the first written is the error the block reports"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
error = "Found -1 for field `n`, with record `s:3`, but field must conform to: $value > 0"

[[test.results]]
error = "Database index `u` already contains 1, with record `t:1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[env]
namespace = "test"
database = "test"

*/

DEFINE INDEX u ON t FIELDS n UNIQUE;
DEFINE FIELD n ON s TYPE int ASSERT $value > 0;
CREATE t:1 SET n = 1;
{ CREATE s:3 SET n = -1; CREATE t:3 SET n = 1; };
{ CREATE t:3 SET n = 1; CREATE s:3 SET n = -1; };
SELECT * FROM s;
SELECT * FROM t;
