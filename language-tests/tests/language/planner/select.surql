/**
[env]

[test]
reason = "Test SELECT with ORDER BY and LIMIT"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 100]
        Sort [ctx: Db] [sort_keys: number ASC, keep: 100]
            TableScan [ctx: Db] [table: record, direction: Forward, topk_pushdown: yes]
''''
*/

EXPLAIN SELECT * FROM record ORDER BY number LIMIT 100;
