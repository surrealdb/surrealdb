/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7539
reason = """
The plans behind the answers pinned in
`7539_plain_index_on_array_field_keeps_whole_value_semantics.surql`. An index
column declared without `*` whose field kind holds only arrays stores one entry
per element, and is planned as the element column it behaves as: a count, an equality, a range and an
`ORDER BY` over the whole value are not answered from it, since the entries
do not hold the value they compare. Nor are `CONTAINS` and `CONTAINSANY`: the
kind does not show that every stored value is an array, and a string stored
whole holds substrings no element seek reaches, so the scan answers them. A
field whose kind excludes arrays keeps every index path, and so does one
whose kind is unknown because an untyped child definition produces its own
value over its parent's object literal type.
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
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: s, direction: Forward, predicate: acl CONTAINS 'a', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: s, direction: Forward, predicate: acl CONTAINSANY ['a', 'c'], pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        TableScan [ctx: Db] [table: s, direction: Forward, predicate: acl CONTAINS 'a', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        TableScan [ctx: Db] [table: s, direction: Forward, predicate: acl = 'a', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: s, direction: Forward, predicate: acl = ['a', 'b'], pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: s, direction: Forward, predicate: acl > 'a', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: acl ASC, keep: 2]
            TableScan [ctx: Db] [table: s, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: s, condition: name = 'a']
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: s, index: s_name, access: >'a', direction: Forward]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: s, index: s_name, direction: Forward, limit: 2]
''''

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
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: ov, index: ov_mt, access: = 'scalar', direction: Forward]
"'''
*/

DEFINE TABLE s SCHEMAFULL;
DEFINE FIELD acl ON s TYPE array<string>;
DEFINE FIELD name ON s TYPE string;
DEFINE INDEX s_acl ON s FIELDS acl;
DEFINE INDEX s_name ON s FIELDS name;

EXPLAIN SELECT VALUE id FROM s WHERE acl CONTAINS 'a';
EXPLAIN SELECT VALUE id FROM s WHERE acl CONTAINSANY ['a', 'c'];
EXPLAIN SELECT count() FROM s WHERE acl CONTAINS 'a' GROUP ALL;
EXPLAIN SELECT count() FROM s WHERE acl = 'a' GROUP ALL;
EXPLAIN SELECT VALUE id FROM s WHERE acl = ['a', 'b'];
EXPLAIN SELECT VALUE id FROM s WHERE acl > 'a';
EXPLAIN SELECT VALUE id FROM s ORDER BY acl LIMIT 2;

EXPLAIN SELECT count() FROM s WHERE name = 'a' GROUP ALL;
EXPLAIN SELECT VALUE id FROM s WHERE name > 'a';
EXPLAIN SELECT VALUE id FROM s ORDER BY name LIMIT 2;

-- An untyped child definition that produces its own value replaces what its
-- parent's object literal type coerced, so its kind is unknown: the column is
-- planned as written and keeps its scalar index paths.
DEFINE TABLE ov SCHEMAFULL;
DEFINE FIELD metadata ON ov TYPE { tags: array<string> };
DEFINE FIELD metadata.tags ON ov VALUE 'scalar';
DEFINE INDEX ov_mt ON ov FIELDS metadata.tags;
EXPLAIN SELECT VALUE id FROM ov WHERE metadata.tags = 'scalar';
