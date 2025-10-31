/**
[test]

[[test.results]]
value = "[{ group: 1, id: test:1, one: 1.7f, two: 2.4f }]"

[[test.results]]
value = "[{ group: 1, id: test:2, one: 4.7f, two: 3.9f }]"

[[test.results]]
value = "[{ group: 2, id: test:3, one: 3.2f, two: 9.7f }]"

[[test.results]]
value = "[{ group: 2, id: test:4, one: 4.4f, two: 3f }]"

[[test.results]]
value = "[{ group: 1, one: 5f, two: 5f }, { group: 2, one: 7f, two: 12f }]"

[[test.results]]
value = "[{ group: 1, one: 7f, two: 6f }, { group: 2, one: 7f, two: 13f }]"

[[test.results]]
value = "[{ group: 1, one: 7f, two: 7f }, { group: 2, one: 9f, two: 13f }]"

[[test.results]]
value = '''[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' }, { detail: { "Aggregate expressions": { expr0: 'math::ceil(one)', expr1: 'math::ceil(two)' }, Aggregations: { _a0: 'Sum(expr0)', _a1: 'Sum(expr1)' }, "Group expressions": { _g0: 'group' }, "Select expression": { group: '_g0', one: '_a0', two: '_a1' }, type: 'Group' }, operation: 'Collector' }]'''

*/
CREATE test:1 SET group = 1, one = 1.7, two = 2.4;
CREATE test:2 SET group = 1, one = 4.7, two = 3.9;
CREATE test:3 SET group = 2, one = 3.2, two = 9.7;
CREATE test:4 SET group = 2, one = 4.4, two = 3.0;
SELECT group, math::sum(math::floor(one)) AS one, math::sum(math::floor(two)) AS two FROM test GROUP BY group;
SELECT group, math::sum(math::round(one)) AS one, math::sum(math::round(two)) AS two FROM test GROUP BY group;
SELECT group, math::sum(math::ceil(one)) AS one, math::sum(math::ceil(two)) AS two FROM test GROUP BY group;
SELECT group, math::sum(math::ceil(one)) AS one, math::sum(math::ceil(two)) AS two FROM test GROUP BY group EXPLAIN;
