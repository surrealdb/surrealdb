/**
[test]

[[test.results]]
value = "[{ bar: 'hello', foo: 'Man', id: table:1 }]"

[[test.results]]
value = "[{ bar: 'hello', foo: 'World', id: table:2 }]"

[[test.results]]
value = "[{ bar: 'world', id: table:3 }]"

[[test.results]]
value = '''[{ detail: { direction: 'forward', table: 'table' }, operation: 'Iterate Table Count' }, { detail: { "Aggregate expressions": {  }, Aggregations: { _a0: 'Count' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'table' }, operation: 'Iterate Table Keys' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ count: 1 }, { count: 1 }, { count: 1 }]"

*/
CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
CREATE table:2 CONTENT { bar: "hello", foo: "World"};
CREATE table:3 CONTENT { bar: "world"};
SELECT COUNT() FROM table GROUP ALL EXPLAIN;
SELECT COUNT() FROM table GROUP ALL;
SELECT COUNT() FROM table EXPLAIN;
SELECT COUNT() FROM table;
