/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An index stores a record holding an array once per element, in any column
	not declared a kind that holds no array. A scan whose key range leaves such
	a column open reaches the record through several entries, and lists it
	once: on a column with no declared kind, on one declared a kind admitting
	an array, and as the trailing column of a compound index seeked on its
	leading column. The rows and the count are the table scan's. A column
	declared a scalar kind holds one entry per record, and its scan carries no
	dedupe.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, k: 1, n: 7, v: [10, 'x'] }, { id: w:2, k: 1, n: 8, v: 10 }, { id: w:3, k: 1, n: 9, v: [10] }]"

[[test.results]]
value = "[w:1, w:2, w:3]"

[[test.results]]
value = "[w:1, w:2, w:3]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[w:1, w:2, w:3]"

[[test.results]]
value = "[w:1, w:2, w:3]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[w:1]"

[[test.results]]
value = "[w:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: m:1, v: [10, 'x'] }]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: s:1, n: 7 }]"

[[test.results]]
value = "[s:1]"

[[test.results]]
value = "false"

*/

DEFINE TABLE w;
DEFINE INDEX ix ON w FIELDS v;
DEFINE INDEX kv ON w FIELDS k, v;
DEFINE INDEX n ON w FIELDS n;
INSERT INTO w [{ id: w:1, k: 1, n: 7, v: [10, 'x'] }, { id: w:2, k: 1, n: 8, v: 10 }, { id: w:3, k: 1, n: 9, v: [10] }];
SELECT VALUE id FROM w WITH NOINDEX WHERE k = 1 ORDER BY id;
SELECT VALUE id FROM w WITH INDEX kv WHERE k = 1 ORDER BY id;
SELECT count() FROM w WITH INDEX kv WHERE k = 1 GROUP ALL;
SELECT VALUE id FROM w WITH NOINDEX WHERE v > 5 ORDER BY id;
SELECT VALUE id FROM w WITH INDEX ix WHERE v > 5 ORDER BY id;
SELECT count() FROM w WITH INDEX ix WHERE v > 5 GROUP ALL;
SELECT VALUE id FROM w WITH NOINDEX WHERE k = 1 AND v = [10, 'x'];
SELECT VALUE id FROM w WITH INDEX kv WHERE k = 1 AND v = [10, 'x'];
DEFINE FIELD v ON m TYPE array<int | string> | int;
DEFINE INDEX ix ON m FIELDS v;
CREATE m:1 SET v = [10, 'x'];
SELECT VALUE id FROM m WITH NOINDEX WHERE v > 5;
SELECT VALUE id FROM m WITH INDEX ix WHERE v > 5;
string::contains(<string> (EXPLAIN SELECT VALUE id FROM w WITH INDEX ix WHERE v > 5), 'DistinctRecords');
DEFINE FIELD n ON s TYPE int;
DEFINE INDEX ix ON s FIELDS n;
CREATE s:1 SET n = 7;
SELECT VALUE id FROM s WITH INDEX ix WHERE n > 5;
string::contains(<string> (EXPLAIN SELECT VALUE id FROM s WITH INDEX ix WHERE n > 5), 'DistinctRecords');
