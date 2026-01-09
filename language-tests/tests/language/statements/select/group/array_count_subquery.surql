/**
[test]

[[test.results]]
value = "[{ bar: 'hello', foo: 'Man', id: table:1 }]"

[[test.results]]
value = "[{ bar: 'hello', foo: 'World', id: table:2 }]"

[[test.results]]
value = "[{ bar: 'world', id: table:3 }]"

[[test.results]]
value = '''[{ detail: { direction: 'forward', table: 'table' }, operation: 'Iterate Table' }, { detail: { "Aggregate expressions": { expr0: 'foo != NONE' }, Aggregations: { _a0: 'CountValue(expr0)' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]'''

[[test.results]]
value = "[{ count: 2 }]"

*/
CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
CREATE table:2 CONTENT { bar: "hello", foo: "World"};
CREATE table:3 CONTENT { bar: "world"};
SELECT COUNT(foo != none) FROM table GROUP ALL EXPLAIN;
SELECT COUNT(foo != none) FROM table GROUP ALL;
