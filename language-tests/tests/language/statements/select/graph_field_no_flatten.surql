/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test that SELECT VALUE ->edge.field does not flatten nested arrays"

[[test.results]]
value = "[{ id: a:1 }, { id: a:2 }]"

[[test.results]]
value = "[{ id: b:1, in: a:1, list: [1, 2, 3], out: a:2 }]"

[[test.results]]
value = "[{ id: b:2, in: a:1, list: [4, 5, 6], out: a:2 }]"

[[test.results]]
value = "[[[1, 2, 3], [4, 5, 6]]]"
*/

CREATE a:1, a:2;
RELATE a:1->b:1->a:2 SET list = [1, 2, 3];
RELATE a:1->b:2->a:2 SET list = [4, 5, 6];
SELECT VALUE ->b.list FROM a:1;
