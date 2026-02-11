/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
└────> Scan [ctx: Db] [source: `table`]
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Scan [ctx: Db] [source: `table`]
"'''

[[test.results]]
value = "[{ count: 1 }, { count: 1 }, { count: 1 }]"

*/
{
    CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
    CREATE table:2 CONTENT { bar: "hello", foo: "World"};
    CREATE table:3 CONTENT { bar: "world"};
    RETURN "OK";
};

EXPLAIN SELECT COUNT() FROM table GROUP ALL;
SELECT COUNT() FROM table GROUP ALL;
EXPLAIN SELECT COUNT() FROM table;
SELECT COUNT() FROM table;
