/**
[test]
reason = "Non-grouped ORDER BY + TEMPFILES routes through the disk-backed ExternalSortByKey operator when the datastore has a temporary_directory configured. The EXPLAIN string is the load-bearing assertion that the operator tree contains ExternalSortByKey rather than SortByKey / SortTopKByKey."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ord:01, n: 1 }, { id: ord:02, n: 2 }, { id: ord:03, n: 3 }, { id: ord:04, n: 4 }, { id: ord:05, n: 5 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id, n]\n    ExternalSortByKey [ctx: Db] [sort_keys: n ASC]\n        TableScan [ctx: Db] [table: ord, direction: Forward]\n''''

*/

{
    CREATE ord:03 SET n = 3;
    CREATE ord:01 SET n = 1;
    CREATE ord:05 SET n = 5;
    CREATE ord:02 SET n = 2;
    CREATE ord:04 SET n = 4;
    RETURN NONE;
};

SELECT id, n FROM ord ORDER BY n ASC TEMPFILES;
EXPLAIN SELECT id, n FROM ord ORDER BY n ASC TEMPFILES;
