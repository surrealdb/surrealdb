/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:{ id: 3, r: o:1 } }]"

[[test.results]]
value = "[{ id: t:{ id: 3, r: o:1 } }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: [o:1, 2025], direction: Forward]
''''

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx, access: [o:2] MoreThan 2022, direction: Forward]
''''

*/
{
    DEFINE INDEX idx ON t FIELDS id.id().r,b UNIQUE;
    CREATE t:{ id: 1, r: o:1 } SET b=2021;
    CREATE t:{ id: 2, r: o:2 } SET b=2021;
    CREATE t:{ id: 3, r: o:1 } SET b=2025;
    CREATE t:{ id: 4, r: o:2 } SET b=2025;
    CREATE t:{ id: 5, r: o:3 } SET b=2025;
    RETURN "OK";
};

SELECT id FROM t WHERE id.id().r=o:1 AND b=2025;
SELECT id FROM t WITH NO INDEX WHERE id.id().r=o:1 AND b=2025;
EXPLAIN SELECT id FROM t WHERE id.id().r=o:1 AND b=2025;

SELECT id FROM t WHERE id.id().r=o:2 AND b>2022;
SELECT id FROM t WITH NO INDEX WHERE id.id().r=o:2 AND b>2022;
EXPLAIN SELECT id FROM t WHERE id.id().r=o:2 AND b>2022;
