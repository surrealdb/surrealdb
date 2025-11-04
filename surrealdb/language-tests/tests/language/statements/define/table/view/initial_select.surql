/**
[test]

[[test.results]]
value = "[{ id: tgt:1, v: 1 }]"

[[test.results]]
value = "[{ id: tgt:2, v: 2 }]"

[[test.results]]
value = "[{ id: tgt:3, v: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ id: view:[], "math::mean": 2f }]'

*/

CREATE tgt:1 SET v = 1;
CREATE tgt:2 SET v = 2;
CREATE tgt:3 SET v = 3;

DEFINE TABLE view AS SELECT math::mean(v) FROM tgt GROUP ALL;
SELECT * FROM view;


