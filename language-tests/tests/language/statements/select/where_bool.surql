/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test WHERE clause with boolean field comparisons"

[[test.results]]
value = "[{ active: false, id: test:1 }]"

[[test.results]]
value = "[{ active: false, id: test:2 }]"

[[test.results]]
value = "[{ active: true, id: test:3 }]"

[[test.results]]
value = "[{ active: false, id: test:1 }, { active: false, id: test:2 }]"

[[test.results]]
value = "[{ active: false, id: test:1 }, { active: false, id: test:2 }]"

[[test.results]]
value = "[{ active: true, id: test:3 }]"
*/

CREATE test:1 SET active = false;
CREATE test:2 SET active = false;
CREATE test:3 SET active = true;
SELECT * FROM test WHERE active = false;
SELECT * FROM test WHERE active != true;
SELECT * FROM test WHERE active = true;
