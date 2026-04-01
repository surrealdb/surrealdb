/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY on a table with a unique compound index must return records even when optional fields are NONE"
issue = 7129

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
value = "[{ i: 2, id: t:2 }]"

[[test.results]]
value = "[{ i: 2, id: t:2 }, { i: 1, id: t:1 }]"

[[test.results]]
value = "[{ i: 1, id: t:1 }, { i: 2, id: t:2 }]"

[[test.results]]
value = "[{ i: 1, id: t:1 }, { i: 2, id: t:2 }]"

[[test.results]]
value = "[{ i: 2, id: t:2 }, { i: 1, id: t:1 }]"
*/

DEFINE TABLE OVERWRITE t SCHEMAFULL;
DEFINE FIELD OVERWRITE i ON t TYPE int;
DEFINE FIELD OVERWRITE j ON t TYPE option<string>;
DEFINE INDEX OVERWRITE t_idx ON t FIELDS i, j UNIQUE;

CREATE t:1 SET i = 1;
CREATE t:2 SET i = 2;

-- Core reproduction: ORDER BY DESC should return both records
SELECT * FROM t ORDER BY i DESC;

-- ORDER BY ASC should also return both records
SELECT * FROM t ORDER BY i ASC;

-- Verify records exist via unordered select
SELECT * FROM t;

-- Verify with a WHERE clause
SELECT * FROM t WHERE i >= 1 ORDER BY i DESC;
