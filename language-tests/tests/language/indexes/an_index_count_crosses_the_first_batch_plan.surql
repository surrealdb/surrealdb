/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "The plans `an_index_count_crosses_the_first_batch` counts through: an index-only count of each index."

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
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: c, condition: n < 1001]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: cu, condition: n < 1001]
''''

*/

DEFINE TABLE c SCHEMALESS;
DEFINE FIELD n ON c TYPE int;
DEFINE INDEX c_n ON c FIELDS n;
DEFINE TABLE cu SCHEMALESS;
DEFINE FIELD n ON cu TYPE int;
DEFINE INDEX cu_n ON cu FIELDS n UNIQUE;
FOR $i IN 0..3 { CREATE type::record('c', $i) SET n = $i; };
FOR $i IN 0..3 { CREATE type::record('cu', $i) SET n = $i; };

EXPLAIN SELECT count() FROM c WHERE n < 1001 GROUP ALL;
EXPLAIN SELECT count() FROM cu WHERE n < 1001 GROUP ALL;
