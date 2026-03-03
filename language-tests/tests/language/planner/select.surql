/**

[test]
reason = "Test SELECT with ORDER BY and LIMIT"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 100]
        SortTopKByKey [ctx: Db] [sort_keys: number ASC, limit: 100]
            TableScan [ctx: Db] [table: record, direction: Forward]
''''
*/

EXPLAIN SELECT * FROM record ORDER BY number LIMIT 100;
