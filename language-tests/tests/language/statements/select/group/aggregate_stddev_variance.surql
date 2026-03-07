/**
[test]
reason = "Test math::stddev and math::variance aggregates with GROUP BY"

[[test.results]]
value = "[{ category: 'a', id: item:1, value: 10 }]"

[[test.results]]
value = "[{ category: 'a', id: item:2, value: 20 }]"

[[test.results]]
value = "[{ category: 'a', id: item:3, value: 30 }]"

[[test.results]]
value = "[{ category: 'b', id: item:4, value: 5 }]"

[[test.results]]
value = "[{ category: 'b', id: item:5, value: 15 }]"

[[test.results]]
value = "[{ category: 'a', sd: 10f, var: 100f }, { category: 'b', sd: 7.0710678118654755f, var: 50f }]"

*/

CREATE item:1 SET category = 'a', value = 10;
CREATE item:2 SET category = 'a', value = 20;
CREATE item:3 SET category = 'a', value = 30;
CREATE item:4 SET category = 'b', value = 5;
CREATE item:5 SET category = 'b', value = 15;

SELECT category, math::stddev(value) AS sd, math::variance(value) AS var FROM item GROUP BY category ORDER BY category;
