/**
[env]
auth = { level = "owner" }

[test]
reason = """
	The kind rule elides a cast to a checked kind only. A stored field's
	declared kind is not checked, so `<int> a` over a field declared `TYPE
	int` is kept; a computed column is coerced to its kind as it lands, so
	`<int> c` over a computed `TYPE int` column is elided; a comparison is a
	`bool`, so `<bool> (a = 1)` is elided.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, c: 2, id: t:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: a, c, flag]
    ProjectLayer [ctx: Db] [columns: c = a * 2 : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = "[{ a: 1, c: 2, flag: true }]"
*/

DEFINE FIELD a ON t TYPE int;
DEFINE FIELD c ON t TYPE int COMPUTED a * 2;
CREATE t:1 SET a = 1;
EXPLAIN SELECT <int> a AS a, <int> c AS c, <bool> (a = 1) AS flag FROM t;
SELECT <int> a AS a, <int> c AS c, <bool> (a = 1) AS flag FROM t;
