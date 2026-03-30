/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "ORDER BY in subquery using graph traversal should correctly sort the results"
issue = 7168

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: edge:x, in: test:test, out: something:1 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: edge:x, in: test:test, out: something:2 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: edge:x, in: test:test, out: something:3 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: edge:x, in: test:test, out: something:4 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: edge:x, in: test:test, out: something:5 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: test:test, relation: [{ id: something:1, orderNumber: 0 }, { id: something:5, orderNumber: 1 }, { id: something:3, orderNumber: 2 }, { id: something:2, orderNumber: 3 }, { id: something:4, orderNumber: 4 }] }]"

[[test.results]]
value = "[{ id: test:test, relation: [{ id: something:4, orderNumber: 4 }, { id: something:2, orderNumber: 3 }, { id: something:3, orderNumber: 2 }, { id: something:5, orderNumber: 1 }, { id: something:1, orderNumber: 0 }] }]"

[[test.results]]
value = "[{ id: something:1, orderNumber: 0 }, { id: something:5, orderNumber: 1 }, { id: something:3, orderNumber: 2 }, { id: something:2, orderNumber: 3 }, { id: something:4, orderNumber: 4 }]"
*/

UPSERT test:test RETURN NONE;

UPSERT something:1 SET orderNumber = 0 RETURN NONE;
UPSERT something:2 SET orderNumber = 3 RETURN NONE;
UPSERT something:3 SET orderNumber = 2 RETURN NONE;
UPSERT something:4 SET orderNumber = 4 RETURN NONE;
UPSERT something:5 SET orderNumber = 1 RETURN NONE;

RELATE test:test->edge->something:1;
RELATE test:test->edge->something:2;
RELATE test:test->edge->something:3;
RELATE test:test->edge->something:4;
RELATE test:test->edge->something:5;

-- Core reproduction: ORDER BY in subquery with graph traversal
SELECT
    *,
    (
        SELECT *
        FROM $parent->edge->something
        ORDER BY orderNumber ASC
    ) AS relation
FROM test:test;

-- ORDER BY DESC variant
SELECT
    *,
    (
        SELECT *
        FROM $parent->edge->something
        ORDER BY orderNumber DESC
    ) AS relation
FROM test:test;

-- Control: direct SELECT with ORDER BY (should always work)
SELECT *
FROM something
ORDER BY orderNumber ASC;
