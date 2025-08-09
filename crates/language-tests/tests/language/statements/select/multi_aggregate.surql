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
value = "[{ group: 1, min: 1.7f, one: 6.4f, two: 6.3f }, { group: 2, min: 3.2f, one: 7.6000000000000005f, two: 12.7f }]"

[[test.results]]
value = "[{ group: 1, max: 3.9f, mean: 3.2f, one: 6.4f, two: 6.3f }, { group: 2, max: 9.7f, mean: 3.8000000000000003f, one: 7.6000000000000005f, two: 12.7f }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' }, { detail: { idioms: { group: ['first'], max: ['math::max'], mean: ['math::mean'], one: ['math::sum'], two: ['math::sum'] }, type: 'Group' }, operation: 'Collector' }]"

*/
CREATE test:1 SET group = 1, one = 1.7, two = 2.4;
CREATE test:2 SET group = 1, one = 4.7, two = 3.9;
CREATE test:3 SET group = 2, one = 3.2, two = 9.7;
CREATE test:4 SET group = 2, one = 4.4, two = 3.0;
SELECT group, math::sum(one) AS one, math::sum(two) AS two, math::min(one) as min FROM test GROUP BY group;
SELECT group, math::sum(two) AS two, math::sum(one) AS one, math::max(two) as max, math::mean(one) as mean FROM test GROUP BY group;
SELECT group, math::sum(two) AS two, math::sum(one) AS one, math::max(two) as max, math::mean(one) as mean FROM test GROUP BY group EXPLAIN;

