/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
A compound B-tree index whose later column flattens a set field
(`FIELDS account, s.*` over `TYPE set<int>`) keys each record by its `account`
and its whole set. The `account` prefix is exact, so a lookup and a count on
`account` are answered correctly, and a containment or an equality over `s`
next to it (`account = 'a' AND s CONTAINS 3`, `s = <set>[1, 2]`) still returns
every matching record. In an index with an array element column before the set
column, containment over the array is unaffected.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ account: 'a', id: t:1, s: {1, 2} }]"

[[test.results]]
value = "[{ account: 'b', id: t:2, s: {2, 3} }]"

[[test.results]]
value = "[{ account: 'a', id: t:3, s: {3,} }]"

[[test.results]]
value = "[t:1, t:3]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[m:1, m:2]"

*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD s ON t TYPE set<int>;
DEFINE INDEX cx ON t FIELDS account, s.*;
CREATE t:1 SET account = 'a', s = <set>[1, 2];
CREATE t:2 SET account = 'b', s = <set>[2, 3];
CREATE t:3 SET account = 'a', s = <set>[3];
SELECT VALUE id FROM t WHERE account = 'a' ORDER BY id;
SELECT count() FROM t WHERE account = 'a' GROUP ALL;
SELECT VALUE id FROM t WHERE account = 'a' AND s CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM t WHERE account = 'a' AND s = <set>[1, 2] ORDER BY id;

-- An array element column and a set column in one index: containment on the
-- array column is answered from the index, on the set column from the rows.
DEFINE TABLE m SCHEMALESS;
DEFINE FIELD tags ON m TYPE array<string>;
DEFINE FIELD s ON m TYPE set<int>;
DEFINE INDEX mx ON m FIELDS tags.*, s.*;
CREATE m:1 SET tags = ['x', 'y'], s = <set>[1, 2] RETURN NONE;
CREATE m:2 SET tags = ['y'], s = <set>[2, 3] RETURN NONE;
SELECT VALUE id FROM m WHERE tags CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM m WHERE tags CONTAINS 'y' AND s CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM m WHERE s CONTAINS 2 ORDER BY id;
