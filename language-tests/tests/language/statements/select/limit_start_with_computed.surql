/**
[test]
reason = "LIMIT/START with computed fields should return correct results (pre_skip pushdown)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ doubled: 0, id: test:1, val: 0 }]"

[[test.results]]
value = "[{ doubled: 2, id: test:2, val: 1 }]"

[[test.results]]
value = "[{ doubled: 4, id: test:3, val: 2 }]"

[[test.results]]
value = "[{ doubled: 6, id: test:4, val: 3 }]"

[[test.results]]
value = "[{ doubled: 8, id: test:5, val: 4 }]"

[[test.results]]
value = "[{ doubled: 10, id: test:6, val: 5 }]"

[[test.results]]
value = "[{ doubled: 12, id: test:7, val: 6 }]"

[[test.results]]
value = "[{ doubled: 14, id: test:8, val: 7 }]"

[[test.results]]
value = "[{ doubled: 16, id: test:9, val: 8 }]"

[[test.results]]
value = "[{ doubled: 18, id: test:10, val: 9 }]"

[[test.results]]
value = "[{ doubled: 14, id: test:8, val: 7 }, { doubled: 16, id: test:9, val: 8 }]"

[[test.results]]
value = "[{ val: 7 }, { val: 8 }]"

[[test.results]]
value = "[]"

*/

DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD val ON test TYPE int;
DEFINE FIELD doubled ON test TYPE int VALUE val * 2;

CREATE test:1 SET val = 0;
CREATE test:2 SET val = 1;
CREATE test:3 SET val = 2;
CREATE test:4 SET val = 3;
CREATE test:5 SET val = 4;
CREATE test:6 SET val = 5;
CREATE test:7 SET val = 6;
CREATE test:8 SET val = 7;
CREATE test:9 SET val = 8;
CREATE test:10 SET val = 9;

-- SELECT * with START offset past most records, LIMIT 2
SELECT * FROM test LIMIT 2 START 7;

-- Same query but selecting only stored fields (no computed fields needed)
SELECT val FROM test LIMIT 2 START 7;

-- SELECT * with START beyond all records
SELECT * FROM test LIMIT 5 START 100;
