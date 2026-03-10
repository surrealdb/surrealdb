/**
[env]
new-planner-strategy = "compute-only"

[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''[{ detail: { direction: 'forward', table: 'table' }, operation: 'Iterate Table Count' }, { detail: { "Aggregate expressions": {  }, Aggregations: { _a0: 'Count' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'table' }, operation: 'Iterate Table Keys' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ count: 1 }, { count: 1 }, { count: 1 }]"

*/
{
    CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
    CREATE table:2 CONTENT { bar: "hello", foo: "World"};
    CREATE table:3 CONTENT { bar: "world"};
    RETURN "OK";
};

SELECT COUNT() FROM table GROUP ALL EXPLAIN;
SELECT COUNT() FROM table GROUP ALL;
SELECT COUNT() FROM table EXPLAIN;
SELECT COUNT() FROM table;
