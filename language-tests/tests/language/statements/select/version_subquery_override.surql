/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "Inner VERSION clause should override outer VERSION in subqueries"

[[test.results]]
value = "[{ id: item:1, name: 'v1' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, name: 'v2' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, name: 'v3' }]"

[[test.results]]
value = "[{ id: item:1, name: 'v3' }]"

[[test.results]]
value = "[{ id: item:1, name: 'v1' }]"

[[test.results]]
value = "[{ id: item:1, latest: [{ id: item:1, name: 'v1' }], name: 'v1' }]"

[[test.results]]
value = "[{ id: item:1, latest: [{ id: item:1, name: 'v2' }], name: 'v1' }]"

*/

-- When a subquery has its own VERSION clause, it should override the
-- outer query's VERSION, not inherit it.

CREATE item:1 SET name = 'v1';

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

UPDATE item:1 SET name = 'v2';

SLEEP 50ms;
LET $t2 = time::now();
SLEEP 50ms;

UPDATE item:1 SET name = 'v3';

-- Sanity: HEAD shows v3
SELECT * FROM item:1;

-- Sanity: VERSION $t1 shows v1
SELECT * FROM item:1 VERSION $t1;

-- Inner with no VERSION inherits outer $t1 — both show v1
SELECT *, (SELECT * FROM item:1) AS latest FROM item:1 VERSION $t1;

-- Inner with explicit VERSION $t2 overrides outer $t1
SELECT *, (SELECT * FROM item:1 VERSION $t2) AS latest FROM item:1 VERSION $t1;
