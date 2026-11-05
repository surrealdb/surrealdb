/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An index on a field holding arrays stores one entry per element, so a key
	equal to a whole array is never one it holds. A comparison to a whole array
	therefore reads the records a scan reads, with or without an index hint.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, k: 1, v: [10, 'x'] }, { id: w:2, k: 1, v: 10 }, { id: w:3, k: 1, v: [10] }]"

[[test.results]]
value = "[w:1]"

[[test.results]]
value = "[w:1]"

[[test.results]]
value = "[w:1]"

[[test.results]]
value = "[w:3]"

[[test.results]]
value = "[w:3]"

[[test.results]]
value = "[w:2]"

[[test.results]]
value = "[w:1, w:3]"

[[test.results]]
value = "[w:1, w:3]"
*/

DEFINE TABLE w;
DEFINE INDEX ix ON w FIELDS v;
INSERT INTO w [{ id: w:1, k: 1, v: [10, 'x'] }, { id: w:2, k: 1, v: 10 }, { id: w:3, k: 1, v: [10] }];
SELECT VALUE id FROM w WITH NOINDEX WHERE v = [10, 'x'];
SELECT VALUE id FROM w WITH INDEX ix WHERE v = [10, 'x'];
SELECT VALUE id FROM w WHERE v = [10, 'x'];
SELECT VALUE id FROM w WITH INDEX ix WHERE v = [10];
SELECT VALUE id FROM w WHERE v = [10];
SELECT VALUE id FROM w WITH INDEX ix WHERE v = 10;
SELECT VALUE id FROM w WITH INDEX ix WHERE v >= [10] ORDER BY id;
SELECT VALUE id FROM w WITH NOINDEX WHERE v >= [10] ORDER BY id;
