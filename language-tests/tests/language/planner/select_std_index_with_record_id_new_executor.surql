/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:[3, o:1] }]"

[[test.results]]
value = "[{ id: t:[3, o:1] }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: t, predicate: id[1] = o:1 AND b = 2025]\n''''

[[test.results]]
value = "[{ id: t:[4, o:2] }]"

[[test.results]]
value = "[{ id: t:[4, o:2] }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: t, predicate: id[1] = o:2 AND b > 2022]\n''''

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
EXPLAIN SELECT id FROM t WHERE id[1]=o:1 AND b=2025;

SELECT id FROM t WITH NO INDEX WHERE id[1]=o:2 AND b>2022;
SELECT id FROM t WHERE id[1]=o:2 AND b>2022;
EXPLAIN SELECT id FROM t WHERE id[1]=o:2 AND b>2022;
