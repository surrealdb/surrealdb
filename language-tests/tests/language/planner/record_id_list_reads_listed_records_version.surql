/**
[env]
namespace = true
database = true
# The TiKV backend does not support versioned reads, and surrealkv
# refuses them unless the datastore is opened versioned.
backend = ["mem", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	`id IN <list>` under a VERSION stamp reads each listed record as it was at
	the stamp: a record created after it is no row, and an updated one has its
	earlier value.
"""

[[test.results]]
value = "[{ id: vt:1, n: 1 }]"

[[test.results]]
value = "[{ id: vt:2, n: 2 }]"

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
value = "[1, 2]"

[[test.results]]
value = "[10, 20, 3]"

[[test.results]]
value = "{ attributes: { version: '$t1' }, children: [{ attributes: { expr: 'n' }, children: [{ attributes: { record_id: '[vt:1, vt:2, vt:3]' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'VersionScope' }"
*/

CREATE vt:1 SET n = 1;
CREATE vt:2 SET n = 2;
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;
UPDATE vt SET n = n * 10 RETURN NONE;
CREATE vt:3 SET n = 3 RETURN NONE;
SELECT VALUE n FROM vt WHERE id IN [vt:3, vt:2, vt:1] VERSION $t1;
SELECT VALUE n FROM vt WHERE id IN [vt:3, vt:2, vt:1];
SELECT VALUE n FROM vt WHERE id IN [vt:3, vt:2, vt:1] VERSION $t1 EXPLAIN;
