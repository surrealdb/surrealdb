/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Aggregate [ctx: Db] [mode: GROUP ALL]
└────> TableScan [ctx: Db] [table: table, direction: Forward]
''''

[[test.results]]
value = "[{ count: 2 }]"

*/
{
    CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
    CREATE table:2 CONTENT { bar: "hello", foo: "World"};
    CREATE table:3 CONTENT { bar: "world"};
    RETURN "OK";
};

EXPLAIN SELECT COUNT(foo != none) FROM table GROUP ALL;
SELECT COUNT(foo != none) FROM table GROUP ALL;
