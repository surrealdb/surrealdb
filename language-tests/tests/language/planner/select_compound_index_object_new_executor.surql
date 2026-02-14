/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:{ id: 3, r: o:1 } }]"

[[test.results]]
value = "[{ id: t:{ id: 3, r: o:1 } }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: t, predicate: id.id().r = o:1 AND b = 2025] {rows: 1, batches: 1}
         actual: [access_method: IndexScan, index: idx, access: Compound([o:1, 2025]), direction: Forward]

Total rows: 1''''

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: t, predicate: id.id().r = o:2 AND b > 2022] {rows: 1, batches: 1}
         actual: [access_method: IndexScan, index: idx, access: Compound([o:2]), direction: Forward]

Total rows: 1''''

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: t, predicate: b >= 2025 AND id.id().r = o:2 AND b > 2020] {rows: 1, batches: 1}
         actual: [access_method: IndexScan, index: idx, access: Compound([o:2]), direction: Forward]

Total rows: 1''''

[[test.results]]
value = "[{ id: t:{ id: 2, r: o:2 } }]"

[[test.results]]
value = "[{ id: t:{ id: 2, r: o:2 } }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: t, predicate: b > 2020 AND id.id().r = o:2 AND b <= 2024] {rows: 1, batches: 1}
         actual: [access_method: IndexScan, index: idx, access: Compound([o:2]), direction: Forward]

Total rows: 1''''

*/
{
    DEFINE INDEX idx ON t FIELDS id.id().r,b;
    CREATE t:{ id: 1, r: o:1 } SET b=2021;
    CREATE t:{ id: 2, r: o:2 } SET b=2021;
    CREATE t:{ id: 3, r: o:1 } SET b=2025;
    CREATE t:{ id: 4, r: o:2 } SET b=2025;
    CREATE t:{ id: 5, r: o:3 } SET b=2025;
    RETURN "OK";
};

SELECT id FROM t WHERE id.id().r=o:1 AND b=2025;
SELECT id FROM t WITH NO INDEX WHERE id.id().r=o:1 AND b=2025;
EXPLAIN ANALYZE SELECT id FROM t WHERE id.id().r=o:1 AND b=2025;

SELECT id FROM t WHERE id.id().r=o:2 AND b>2022;
SELECT id FROM t WITH NO INDEX WHERE id.id().r=o:2 AND b>2022;
EXPLAIN ANALYZE SELECT id FROM t WHERE id.id().r=o:2 AND b>2022;

SELECT id FROM t WHERE b>=2025 AND id.id().r=o:2 AND b>2020;
SELECT id FROM t WITH NO INDEX WHERE b>=2025 AND id.id().r=o:2 AND b>2020;
EXPLAIN ANALYZE SELECT id FROM t WHERE b>=2025 AND id.id().r=o:2 AND b>2020;

SELECT id FROM t WHERE b>2020 AND id.id().r=o:2 AND b<=2024;
SELECT id FROM t WITH NO INDEX WHERE b>=2021 AND id.id().r=o:2 AND b<=2024;
EXPLAIN ANALYZE SELECT id FROM t WHERE b>2020 AND id.id().r=o:2 AND b<=2024;
