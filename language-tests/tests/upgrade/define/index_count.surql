/**
[env]
imports = ["upgrade/define/index_count_import.surql"]

[test]
upgrade = true
reason = "COUNT aggregation index reflects pre-upgrade row count, and remains correct after post-upgrade mutations"

[[test.results]]
match = '''$result.indexes.ct_idx == "DEFINE INDEX ct_idx ON ct COUNT"'''

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ id: ct:5, b: true }]"

[[test.results]]
value = "[{ count: 5 }]"

*/
INFO FOR TABLE ct;
SELECT count() FROM ct GROUP ALL;
CREATE ct:5 SET b = true;
SELECT count() FROM ct GROUP ALL;
