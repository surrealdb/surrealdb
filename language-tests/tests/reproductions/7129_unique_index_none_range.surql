/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Range queries against NONE on a unique index must compute correct scan boundaries"
issue = 7129

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ col: 5, id: t:2 }]"

[[test.results]]
value = "[{ col: 5, id: t:2 }]"

[[test.results]]
value = "[{ id: t:1 }, { col: 5, id: t:2 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE OVERWRITE t SCHEMAFULL;
DEFINE FIELD OVERWRITE col ON t TYPE option<int>;
DEFINE INDEX OVERWRITE idx ON t FIELDS col UNIQUE;

CREATE t:1;
CREATE t:2 SET col = 5;

-- Range queries with explicit NONE bounds on a unique index
SELECT * FROM t WHERE col > NONE;
SELECT * FROM t WHERE col >= NONE;
SELECT * FROM t WHERE col <= NONE;
SELECT * FROM t WHERE col < NONE;
