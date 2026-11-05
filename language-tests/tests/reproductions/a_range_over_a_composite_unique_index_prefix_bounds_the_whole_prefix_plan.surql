/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["reproductions/a_range_over_a_composite_unique_index_prefix_bounds_the_whole_prefix_import.surql"]

[test]
reason = """
The plan companion of
`a_range_over_a_composite_unique_index_prefix_bounds_the_whole_prefix`. A
comparison on the leading column of the composite index, UNIQUE or not, is read
as a range of the index's keys with no filter above it, forwards and backwards,
and a count over it is counted from the index. With no filter to judge the rows
again, the rows of that test are exactly the keys the range covers, which is
what makes them a check of the range.
"""

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        IndexScan [ctx: Db] [table: tu, index: uu, access: >'y', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        IndexScan [ctx: Db] [table: tu, index: uu, access: <='y', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: g]
    IndexScan [ctx: Db] [table: tu, index: uu, access: >'y', direction: Backward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: tu, condition: g > 'y']
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        IndexScan [ctx: Db] [table: tc, index: ug, access: >'y', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        IndexScan [ctx: Db] [table: tc, index: ug, access: <='y', direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: g]
    IndexScan [ctx: Db] [table: tc, index: ug, access: >'y', direction: Backward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: tc, condition: g > 'y']
"'''
*/

EXPLAIN SELECT VALUE id FROM tu WHERE g > 'y' ORDER BY id;
EXPLAIN SELECT VALUE id FROM tu WHERE g <= 'y' ORDER BY id;
EXPLAIN SELECT VALUE g FROM tu WHERE g > 'y' ORDER BY g DESC;
EXPLAIN SELECT count() FROM tu WHERE g > 'y' GROUP ALL;

EXPLAIN SELECT VALUE id FROM tc WHERE g > 'y' ORDER BY id;
EXPLAIN SELECT VALUE id FROM tc WHERE g <= 'y' ORDER BY id;
EXPLAIN SELECT VALUE g FROM tc WHERE g > 'y' ORDER BY g DESC;
EXPLAIN SELECT count() FROM tc WHERE g > 'y' GROUP ALL;
