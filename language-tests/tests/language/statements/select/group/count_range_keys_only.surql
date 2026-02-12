/**
[env]
new-planner-strategy = "best-effort-ro"

[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''[{ detail: { direction: 'forward', range: 1..4, table: 'table' }, operation: 'Iterate Range Count' }, { detail: { "Aggregate expressions": {  }, Aggregations: { _a0: 'Count' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', range: 1..4, table: 'table' }, operation: 'Iterate Range Keys' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ count: 1 }, { count: 1 }, { count: 1 }]"

*/

{
    CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
    CREATE table:2 CONTENT { bar: "hello", foo: "World"};
    CREATE table:3 CONTENT { bar: "world"};
    RETURN "OK";
};

SELECT COUNT() FROM table:1..4 GROUP ALL EXPLAIN;
SELECT COUNT() FROM table:1..4 GROUP ALL;
SELECT COUNT() FROM table:1..4 EXPLAIN;
SELECT COUNT() FROM table:1..4;
