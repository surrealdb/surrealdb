/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Ensure link.* works correctly with and without WHERE clause"
issue = 3510

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1, link: a:1, num: 1 }]"

[[test.results]]
value = "[{ link: { id: a:1 } }]"

[[test.results]]
value = "[{ link: { id: a:1 } }]"
*/

CREATE a:1;
CREATE b:1 SET link = a:1, num = 1;
SELECT link.* FROM b;
SELECT link.* FROM b WHERE num = 1;
