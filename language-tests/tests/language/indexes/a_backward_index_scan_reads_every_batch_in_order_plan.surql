/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "The plans `a_backward_index_scan_reads_every_batch_in_order` reads its rows through: a backward scan of each index, with no sort above it."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: s, index: s_n, access: >200 <=1400, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: s, index: s_n, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: s, index: s_n, access: >100, direction: Backward, limit: 3, offset: 1100]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: u, index: u_n, access: >200 <=1400, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: u, index: u_n, direction: Backward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    IndexScan [ctx: Db] [table: u, index: u_n, access: >100, direction: Backward, limit: 3, offset: 1100]
''''

*/

DEFINE TABLE s SCHEMALESS;
DEFINE FIELD n ON s TYPE int;
DEFINE INDEX s_n ON s FIELDS n;
DEFINE TABLE u SCHEMALESS;
DEFINE FIELD n ON u TYPE int;
DEFINE INDEX u_n ON u FIELDS n UNIQUE;
FOR $i IN 0..3 { CREATE type::record('s', $i) SET n = $i; };
FOR $i IN 0..3 { CREATE type::record('u', $i) SET n = $i; };

EXPLAIN SELECT VALUE n FROM s WHERE n > 200 AND n <= 1400 ORDER BY n DESC;
EXPLAIN SELECT VALUE n FROM s ORDER BY n DESC;
EXPLAIN SELECT VALUE n FROM s WHERE n > 100 ORDER BY n DESC LIMIT 3 START 1100;
EXPLAIN SELECT VALUE n FROM u WHERE n > 200 AND n <= 1400 ORDER BY n DESC;
EXPLAIN SELECT VALUE n FROM u ORDER BY n DESC;
EXPLAIN SELECT VALUE n FROM u WHERE n > 100 ORDER BY n DESC LIMIT 3 START 1100;
