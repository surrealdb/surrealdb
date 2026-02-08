/**
[env]
new-planner-strategy = "best-effort-ro"

[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''[{ detail: { direction: 'forward', table: 'table' }, operation: 'Iterate Table' }, { detail: { "Aggregate expressions": { expr0: 'foo != NONE' }, Aggregations: { _a0: 'CountValue(expr0)' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]'''

[[test.results]]
value = "[{ count: 2 }]"

*/
{
    CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
    CREATE table:2 CONTENT { bar: "hello", foo: "World"};
    CREATE table:3 CONTENT { bar: "world"};
    RETURN "OK";
};

SELECT COUNT(foo != none) FROM table GROUP ALL EXPLAIN;
SELECT COUNT(foo != none) FROM table GROUP ALL;
