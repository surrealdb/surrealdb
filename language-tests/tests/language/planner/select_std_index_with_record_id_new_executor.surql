/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:[3, o:1] }]"

[[test.results]]
value = "[{ id: t:[3, o:1] }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1}
    Filter [ctx: Db] [predicate: id[1] = o:1 AND b = 2025] {rows: 1}
        IndexScan [ctx: Db] [index: idx, access: [o:1, 2025], direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ id: t:[4, o:2] }]"

[[test.results]]
value = "[{ id: t:[4, o:2] }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1}
    Filter [ctx: Db] [predicate: id[1] = o:2 AND b > 2022] {rows: 1}
        IndexScan [ctx: Db] [index: idx, access: [o:2], direction: Forward] {rows: 2}

Total rows: 1''''

*/

{
    DEFINE INDEX idx ON t FIELDS id[1],b UNIQUE;
    CREATE t:[ 1, o:1 ] SET b=2021;
    CREATE t:[ 2, o:2 ] SET b=2021;
    CREATE t:[ 3, o:1 ] SET b=2025;
    CREATE t:[ 4, o:2 ] SET b=2025;
    CREATE t:[ 5, o:3 ] SET b=2025;
    RETURN "OK";
};

SELECT id FROM t WITH NO INDEX WHERE id[1]=o:1 AND b=2025;
SELECT id FROM t WHERE id[1]=o:1 AND b=2025;
EXPLAIN ANALYZE SELECT id FROM t WHERE id[1]=o:1 AND b=2025;

SELECT id FROM t WITH NO INDEX WHERE id[1]=o:2 AND b>2022;
SELECT id FROM t WHERE id[1]=o:2 AND b>2022;
EXPLAIN ANALYZE SELECT id FROM t WHERE id[1]=o:2 AND b>2022;
