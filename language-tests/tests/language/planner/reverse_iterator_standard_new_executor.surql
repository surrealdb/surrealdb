/**
[test]
reason = "Tests reverse iteration with a standard index using ORDER BY DESC (new executor)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 3}
    Limit [ctx: Db] [limit: 3] {rows: 3}
        SortTopKByKey [ctx: Db] [sort_keys: v DESC, limit: 3] {rows: 3}
            IndexScan [ctx: Db] [index: idx, access: >5, direction: Forward] {rows: 10}

Total rows: 3''''

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 3}
    Limit [ctx: Db] [limit: 3] {rows: 3}
        SortTopKByKey [ctx: Db] [sort_keys: v DESC, limit: 3] {rows: 3}
            IndexScan [ctx: Db] [index: idx, access: , direction: Forward] {rows: 15}

Total rows: 3''''

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 15}
    SortByKey [ctx: Db] [sort_keys: v DESC] {rows: 15}
        IndexScan [ctx: Db] [index: idx, access: , direction: Forward] {rows: 15}

Total rows: 15''''

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }, { v: 12 }, { v: 11 }, { v: 10 }, { v: 9 }, { v: 8 }, { v: 7 }, { v: 6 }, { v: 5 }, { v: 4 }, { v: 3 }, { v: 2 }, { v: 1 }]"
*/

{
    DEFINE INDEX idx ON TABLE i COLUMNS v;
    FOR $i IN 1..=15 { CREATE i:[$i] SET v = $i; };
    RETURN "OK";
};

EXPLAIN ANALYZE SELECT v FROM i WHERE v > 5 ORDER BY v DESC LIMIT 3;
SELECT v FROM i WHERE v > 5 ORDER BY v DESC LIMIT 3;
EXPLAIN ANALYZE SELECT v FROM i ORDER BY v DESC LIMIT 3;
SELECT v FROM i ORDER BY v DESC LIMIT 3;
EXPLAIN ANALYZE SELECT v FROM i ORDER BY v DESC;
SELECT v FROM i ORDER BY v DESC;
