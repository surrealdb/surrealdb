/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
The plan companion of `7460_set_containment_reads_the_documents`: a
flattened index over a field declared as a set does not serve a containment
written with the flatten, which scans the table, while the same index over an
array field still serves it.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: s.* CONTAINS 3]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: a, index: ix, access: = 3, direction: Forward]
''''
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD s ON t TYPE set<int>;
DEFINE INDEX ix ON t FIELDS s.*;
DEFINE TABLE a SCHEMALESS;
DEFINE FIELD s ON a TYPE array<int>;
DEFINE INDEX ix ON a FIELDS s.*;
EXPLAIN SELECT id FROM t WHERE s.* CONTAINS 3;
EXPLAIN SELECT id FROM a WHERE s.* CONTAINS 3;
