/**
[test]
reason = "Test count aggregate with NULL values and conditional expressions"

[[test.results]]
value = "[{ active: true, category: 'a', id: item:1 }]"

[[test.results]]
value = "[{ active: false, category: 'a', id: item:2 }]"

[[test.results]]
value = "[{ active: true, category: 'a', id: item:3 }]"

[[test.results]]
value = "[{ category: 'b', id: item:4 }]"

[[test.results]]
value = "[{ active: true, category: 'b', id: item:5 }]"

[[test.results]]
value = "[{ category: 'a', total: 3 }, { category: 'b', total: 2 }]"

*/

CREATE item:1 SET category = 'a', active = true;
CREATE item:2 SET category = 'a', active = false;
CREATE item:3 SET category = 'a', active = true;
CREATE item:4 SET category = 'b', active = NONE;
CREATE item:5 SET category = 'b', active = true;

SELECT category, count() AS total FROM item GROUP BY category ORDER BY category;
