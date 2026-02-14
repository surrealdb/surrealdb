/**
[test]
reason = "Tests reverse iteration with range queries using ORDER BY DESC (new executor)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:[10] }, { id: t:[9] }, { id: t:[8] }]"

[[test.results]]
value = "[{ id: t:[9] }, { id: t:[8] }, { id: t:[7] }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 3, batches: 1}
└────> Limit [ctx: Db] [limit: 3] {rows: 3, batches: 1}
       └────> SortTopKByKey [ctx: Db] [sort_keys: id DESC, limit: 3] {rows: 3, batches: 1}
              └────> RecordIdScan [ctx: Db] [record_id: t:[5]..=[10]] {rows: 6, batches: 1}

Total rows: 3''''
*/

{
    FOR $i IN 1..15 { CREATE t:[$i]; };
    RETURN "OK";
};

SELECT * FROM t:[5]..=[10] ORDER BY id DESC LIMIT 3;
SELECT * FROM t:[5]..[10] ORDER BY id DESC LIMIT 3;
EXPLAIN ANALYZE SELECT * FROM t:[5]..=[10] ORDER BY id DESC LIMIT 3;
