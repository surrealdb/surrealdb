/**
[env]
namespace = true
database = true
auth = { level = "owner" }
versioned = true

[test]
issue = 7460
reason = """
A versioned read returns the same records for a containment over a set field as
a current read does, although a B-tree index that flattens a set field holds one
entry per stored set and so cannot serve an element lookup at any version. This
holds whether the table is named in the statement or resolved at run time
(`type::table($tb)`, with `$tb` bound in the block). Versioning needs a backend
that keeps history, so this test runs on RocksDB and SurrealKV only.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, s: {1, 2, 3} }]"

[[test.results]]
value = "[{ id: t:2, s: {3, 4} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:2]"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD s ON t TYPE set<int>;
DEFINE INDEX ix ON t FIELDS s.*;
CREATE t:1 SET s = <set>[1, 2, 3];
CREATE t:2 SET s = <set>[3, 4];
-- The stamp is taken after the writes have committed, so a read at it sees both records.
SLEEP 50ms;
LET $ts = time::now();
SELECT VALUE id FROM t WHERE s.* CONTAINS 3 ORDER BY id VERSION $ts;
{ LET $tb = 't'; SELECT VALUE id FROM type::table($tb) WHERE s.* CONTAINS 3 ORDER BY id VERSION $ts };
{ LET $tb = 't'; SELECT VALUE id FROM type::table($tb) WHERE s CONTAINSANY [1, 4] ORDER BY id VERSION $ts };
{ LET $tb = 't'; SELECT VALUE id FROM type::table($tb) WHERE 4 IN s ORDER BY id VERSION $ts };
