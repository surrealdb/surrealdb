/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''CountScan [ctx: Db] [source: table:1..4]
''''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = ''''Project [ctx: Db]
└────> DynamicScan [ctx: Db] [source: table:1..4]
''''

[[test.results]]
value = "[{ count: 1 }, { count: 1 }, { count: 1 }]"

*/

{
    CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
    CREATE table:2 CONTENT { bar: "hello", foo: "World"};
    CREATE table:3 CONTENT { bar: "world"};
    RETURN "OK";
};

EXPLAIN SELECT COUNT() FROM table:1..4 GROUP ALL;
SELECT COUNT() FROM table:1..4 GROUP ALL;
EXPLAIN SELECT COUNT() FROM table:1..4;
SELECT COUNT() FROM table:1..4;
