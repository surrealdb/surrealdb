/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db]
└────> Limit [ctx: Db] [limit: 3, offset: 2]
       └────> TableScan [ctx: Db] [table: i, direction: Forward, limit: 3, offset: 2]
''''

[[test.results]]
value = "[{ b: true, id: i:4 }]"

[[test.results]]
value = ''''Project [ctx: Db]
└────> Limit [ctx: Db] [limit: 3, offset: 2]
       └────> Filter [ctx: Db] [predicate: b = true]
              └────> TableScan [ctx: Db] [table: i, direction: Forward, predicate: b = true, limit: 3, offset: 2]
''''

[[test.results]]
value = "[{ b: true, id: i:4 }]"

*/

{
    CREATE i:0 SET b=true;
    CREATE i:1 SET b=true;
    CREATE i:2 SET b=true;
    CREATE i:3 SET b=true;
    CREATE i:4 SET b=true;
    CREATE i:5 SET b=true;
    RETURN "OK";
};

EXPLAIN SELECT * FROM i LIMIT 3 START 2;
SELECT * FROM i LIMIT 3 START 2;
EXPLAIN SELECT * FROM i WHERE b = true LIMIT 3 START 2;
SELECT * FROM i WHERE b = true LIMIT 3 START 2;
