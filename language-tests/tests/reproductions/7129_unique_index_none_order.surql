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
value = "[{ i: 3, id: t:3, j: 'hello' }]"

[[test.results]]
value = "[{ i: 3, id: t:3, j: 'hello' }, { i: 2, id: t:2 }, { i: 1, id: t:1 }]"

[[test.results]]
value = "[{ i: 1, id: t:1 }, { i: 2, id: t:2 }, { i: 3, id: t:3, j: 'hello' }]"

[[test.results]]
value = "[{ i: 1, id: t:1 }, { i: 2, id: t:2 }, { i: 3, id: t:3, j: 'hello' }]"

[[test.results]]
value = "[{ i: 3, id: t:3, j: 'hello' }, { i: 2, id: t:2 }, { i: 1, id: t:1 }]"

[[test.results]]
value = "[{ i: 2, id: t:2 }, { i: 3, id: t:3, j: 'hello' }]"
*/

DEFINE TABLE OVERWRITE t SCHEMAFULL;
DEFINE FIELD OVERWRITE i ON t TYPE int;
DEFINE FIELD OVERWRITE j ON t TYPE option<string>;
DEFINE INDEX OVERWRITE t_idx ON t FIELDS i, j UNIQUE;

CREATE t:1 SET i = 1;
CREATE t:2 SET i = 2;
CREATE t:3 SET i = 3, j = "hello";

-- Core reproduction: ORDER BY DESC should return all records
SELECT * FROM t ORDER BY i DESC;

-- ORDER BY ASC should also return all records
SELECT * FROM t ORDER BY i ASC;

-- Verify records exist via unordered select
SELECT * FROM t;

-- WHERE on the non-NONE record
SELECT * FROM t WHERE i >= 1 ORDER BY i DESC;

-- WHERE on a range that includes NONE-valued records
SELECT * FROM t WHERE i >= 2 ORDER BY i ASC;
