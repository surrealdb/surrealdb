/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
The plan companion of
`7460_a_compound_index_with_a_set_column_serves_its_other_columns`. The column
that flattens a set cannot answer an element lookup, but the exact `account`
prefix before it still serves `account = 'a'` as an index scan, not a table
scan, with a containment over the set kept as a filter. An array element column
before the set column is still sought for containment.
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
value = '''
"SelectProject [ctx: Db] [projections: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [table: t, index: cx, access: ['a'], direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: account = 'a' AND s CONTAINS 3]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: t, index: cx, access: ['a'], direction: Forward]
"'''

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
"SelectProject [ctx: Db] [projections: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [table: m, index: mx, access: ['x'], direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: s.* CONTAINS 2]
        TableScan [ctx: Db] [table: m, direction: Forward]
"'''

*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD s ON t TYPE set<int>;
DEFINE INDEX cx ON t FIELDS account, s.*;
CREATE t:1 SET account = 'a', s = <set>[1, 2];
CREATE t:2 SET account = 'b', s = <set>[2, 3];
CREATE t:3 SET account = 'a', s = <set>[3];
EXPLAIN SELECT id FROM t WHERE account = 'a';
EXPLAIN SELECT id FROM t WHERE account = 'a' AND s CONTAINS 3;

-- The array element column before the set column is still sought; the set
-- column is not.
DEFINE TABLE m SCHEMALESS;
DEFINE FIELD tags ON m TYPE array<string>;
DEFINE FIELD s ON m TYPE set<int>;
DEFINE INDEX mx ON m FIELDS tags.*, s.*;
EXPLAIN SELECT id FROM m WHERE tags.* CONTAINS 'x';
EXPLAIN SELECT id FROM m WHERE s.* CONTAINS 2;
